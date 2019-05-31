/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.linking

import com.codahale.metrics.annotation.Timed
import com.google.common.base.Stopwatch
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.aggregation.Aggregators
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.data.EntityDataKey
import com.openlattice.data.EntityKeyIdService
import com.openlattice.edm.EntitySet
import com.openlattice.edm.PostgresEdmManager
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.HazelcastQueue
import com.openlattice.linking.clustering.ClusterUpdate
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.sql.Connection
import java.time.Instant
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import java.util.stream.Stream


/**
 * Entity sets ids are assigned by calling [UUID.randomUUID] as a result we know that this can never be accidentally
 * assigned to any real entity set.
 */
internal val LINKING_ENTITY_SET_ID = UUID(0, 0)
internal const val PERSON_FQN = "general.person"
internal const val REFRESH_PROPERTY_TYPES_INTERVAL_MILLIS = 30000L
internal const val LINKING_BATCH_TIMEOUT_MILLIS = 120000L
internal const val MINIMUM_SCORE = 0.75

/**
 * Performs realtime linking of individuals as they are integrated ino the system.
 */
@Component
class BackgroundLinkingService
(
        private val executor: ListeningExecutorService,
        private val hazelcastInstance: HazelcastInstance,
        private val pgEdmManager: PostgresEdmManager,
        private val blocker: Blocker,
        private val matcher: Matcher,
        private val ids: EntityKeyIdService,
        private val loader: DataLoader,
        private val gqs: LinkingQueryService,
        private val linkingFeedbackService: PostgresLinkingFeedbackService,
        private val linkableTypes: Set<UUID>,
        private val configuration: LinkingConfiguration
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundLinkingService::class.java)
    }

    //private val entityLockingLock = hazelcastInstance.cpSubsystem.getLock("")
    private val running = ReentrantLock()

    private val blacklist: Set<UUID> = configuration.blacklist
    private val whitelist: Optional<Set<UUID>> = configuration.whitelist


    private val entitySets = hazelcastInstance.getMap<UUID, EntitySet>(HazelcastMap.ENTITY_SETS.name)
    private val linkingLocks = hazelcastInstance.getMap<UUID, String>(HazelcastMap.LINKING_LOCKS.name)

    private val entityLinkingLocks = hazelcastInstance.getMap<EntityDataKey, Long>(HazelcastMap.ENTITY_LINKING_LOCKS.name)

    private val cursors = hazelcastInstance.getMap<UUID, DelegatedUUIDSet>(HazelcastMap.LINKING_CURSORS.name)
    private val candidates = hazelcastInstance.getQueue<EntityDataKey>(HazelcastQueue.LINKING_CANDIDATES.name)


    private val linkingWorker = executor.submit {
        Stream.generate { candidates.take() }
                .parallel()
                .forEach { candidate ->
                    try {
                        lock(candidate)
                        link(candidate)
                    } catch (ex: Exception) {
                        logger.error("Unable to link $candidate. ", ex)
                    } finally {
                        unlock(candidate)
                    }
                }
    }


    /**
     * Links a candidate entity to other matching entities.
     *
     * 1) Uses the results of blocking to identify candidate clusters
     * 2) Insert the results of the match scores
     * 3) Update the linked entities table.
     *
     * @param candidate The data key for the entity to perform linking upon.
     */
    private fun link(candidate: EntityDataKey) {
        clearNeighborhoods(candidate)
        // if we have positive feedbacks on entity, we use its linking id and match them together
        if (hasPositiveFeedback(candidate)) {
            try {
                // only linking id of entity should remain, since we cleared neighborhood, except the ones
                // with positive feedback
                val clusters = getClusters(setOf(candidate))
                val clusterId = clusters.keys.first()
                val cluster = clusters.entries.first()

                gqs.lockClustersForUpdates(setOf(clusterId)).use {
                    val scoredCluster = cluster(candidate, cluster, ::completeLinkCluster)
                    if (scoredCluster.score <= MINIMUM_SCORE) {
                        logger.error(
                                "Recalculated score {} of linking id {} with positives feedbacks did not pass minimum score {}",
                                scoredCluster.score,
                                cluster.key,
                                MINIMUM_SCORE
                        )
                    }

                    val clusterUpdate = ClusterUpdate(scoredCluster.clusterId, candidate, scoredCluster.cluster)

                    insertMatches(clusterUpdate, it)
                }
            } catch (ex: Exception) {
                logger.error("An error occurred while performing linking.", ex)
                throw IllegalStateException("Error occured while performing linking.", ex)
            }
        } else {
            // Run standard blocking + clustering
            val sw = Stopwatch.createStarted()
            val initialBlock = blocker.block(candidate.entitySetId, candidate.entityKeyId)

            logger.info(
                    "Blocking ({}, {}) took {} ms.",
                    candidate.entitySetId,
                    candidate.entityKeyId,
                    sw.elapsed(TimeUnit.MILLISECONDS)
            )


            if (isLocked(initialBlock.second.keys - candidate)) {
                return
            }

            //block contains element being blocked
            val elem = initialBlock.second.getValue(candidate)

            // initialize
            sw.reset().start()
            logger.info("Initializing matching for block {}", candidate)
            val initializedBlock = matcher.initialize(initialBlock)
            logger.info("Initialization took {} ms", sw.elapsed(TimeUnit.MILLISECONDS))
            val dataKeys = collectKeys(initializedBlock.second)

            //Decision that needs to be made is whether to start new cluster or merge into existing cluster.
            //No locks are required since any items that block to this element will be skipped.
            try {
                val clusters = getClusters(dataKeys)
                gqs.lockClustersForUpdates(clusters.keys).use {
                    var maybeBestCluster = clusters
                            .asSequence()
                            .map { cluster -> cluster(candidate, cluster, ::completeLinkCluster) }
                            .filter { scoredCluster -> scoredCluster.score > MINIMUM_SCORE }
                            .maxBy { scoredCluster -> scoredCluster.score }

                    //TODO: When creating new cluster do we really need to re-match or can we assume score of 1.0?
                    val clusterUpdate = if (maybeBestCluster == null) {
                        val clusterId = ids.reserveIds(LINKING_ENTITY_SET_ID, 1).first()
                        val block = candidate to mapOf(candidate to elem)
                        ClusterUpdate(clusterId, candidate, matcher.match(block).second)
                    } else {
                        val bestCluster = maybeBestCluster!!
                        ClusterUpdate(bestCluster.clusterId, candidate, bestCluster.cluster)
                    }

                    insertMatches(clusterUpdate, it)
                }

            } catch (ex: Exception) {
                logger.error("An error occurred while performing linking.", ex)
                throw IllegalStateException("Error occured while performing linking.", ex)
            }
        }
    }


    private fun isLocked(block: Set<EntityDataKey>): Boolean {
        //Skip locked elements as they will be requeued.
        val keyFilter = Predicates.`in`("__key", *block.toTypedArray()) as Predicate<EntityDataKey, Long>

        return entityLinkingLocks.aggregate(Aggregators.count(), keyFilter) > 0L
    }

    private fun cluster(
            blockKey: EntityDataKey,
            identifiedCluster: Map.Entry<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>>,
            clusteringStrategy: (Map<EntityDataKey, Map<EntityDataKey, Double>>) -> Double
    ): ScoredCluster {
        val block = blockKey to loader.getEntities(collectKeys(identifiedCluster.value) + blockKey)
        //At some point, we may want to skip recomputing matches for existing cluster elements as an optimization.
        //Since we're freshly loading entities it's not too bad to recompute everything.
        val matchedBlock = matcher.match(block)
        val matchedCluster = matchedBlock.second
        val score = clusteringStrategy(matchedCluster)
        return ScoredCluster(identifiedCluster.key, matchedCluster, score)
    }


    private fun <T> collectKeys(m: Map<EntityDataKey, Map<EntityDataKey, T>>): Set<EntityDataKey> {
        return m.keys + m.values.flatMap { it.keys }
    }

    private fun hasPositiveFeedback(entity: EntityDataKey): Boolean {
        return linkingFeedbackService.getLinkingFeedbackOnEntity(FeedbackType.Positive, entity).iterator().hasNext()
    }

    private fun clearNeighborhoods(candidate: EntityDataKey) {
        logger.debug("Starting neighborhood cleanup of {}", candidate)
        val positiveFeedbacks = linkingFeedbackService.getLinkingFeedbackOnEntity(FeedbackType.Positive, candidate)
                .map(EntityLinkingFeedback::entityPair)
        val clearedCount = gqs.deleteNeighborhood(candidate, positiveFeedbacks)
        logger.debug("Cleared {} neighbors from neighborhood of {}", clearedCount, candidate)
    }


    /**
     * Retrieve the clusters containing any of the provided data keys.
     * @param dataKeys The entity data keys used to determine which clusters to return.
     * @return A map of pre-scored clusters associated with the given data keys.
     */
    private fun getClusters(dataKeys: Set<EntityDataKey>): Map<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>> {
        return gqs.getClusters(gqs.getIdsOfClustersContaining(dataKeys).toList())
    }

    private fun insertMatches(clusterUpdate: ClusterUpdate, connection: Connection) {
        gqs.insertMatchScores(connection, clusterUpdate.clusterId, clusterUpdate.scores)
        gqs.updateLinkingTable(clusterUpdate.clusterId, clusterUpdate.newMember)
    }

    @Timed
    @Scheduled(fixedRate = 30000)
    fun pruneFailedNodes() {
        logger.info("Removing locks for any failed nodes.")
        val lockOwners = linkingLocks.aggregate(Aggregators.distinct<MutableMap.MutableEntry<UUID, String>, String>())
        val memberIds = hazelcastInstance.cluster.members.map { it.uuid }
        val failedNodes = lockOwners - memberIds
        logger.info("Detected the following failed nodes holding locks: {}", failedNodes)
        linkingLocks.removeAll(Predicates.`in`("this", *failedNodes.toTypedArray()) as Predicate<UUID, String>)
    }

    @Timed
    @Scheduled(fixedRate = 30000)
    fun updateCandidateList() {
        logger.info("Updating linking candidates list.")
        if (running.tryLock()) {
            try {
                pgEdmManager.allEntitySets.asSequence()
                        .filter { linkableTypes.contains(it.entityTypeId) }
                        .filterNot { blacklist.contains(it.id) }
                        .filter { entitySet -> whitelist.map { it.contains(entitySet.id) }.orElse(true) }
                        .filter {
                            //Filter any entity sets that are currently locked for a call to entities needing linking.
                            val ownerId = linkingLocks.putIfAbsent(
                                    it.id,
                                    hazelcastInstance.localEndpoint.uuid
                            ) ?: hazelcastInstance.localEndpoint.uuid
                            return@filter ownerId == hazelcastInstance.localEndpoint.uuid
                        }.forEach {
                            updateCandidateList(it.id)
                        }
            } catch (ex: Exception) {
                logger.info("Encountered error while updating candidates for linking.", ex)
            } finally {
                running.unlock()
            }
        }
    }

    private fun updateCandidateList(entitySetId: UUID) {
        logger.info(
                "Registering job to queue entities needing linking in entity set {} ({}) .",
                entitySets.getValue(entitySetId).name,
                entitySetId
        )

        executor.submit {
            var entitiesNeedingLinking =
                    gqs
                            .getEntitiesNeedingLinking(setOf(entitySetId), configuration.loadSize)
                            .map { EntityDataKey(it.first, it.second) }
            while (entitiesNeedingLinking.isNotEmpty()) {
                entitiesNeedingLinking.forEach(candidates::put)
                entitiesNeedingLinking =
                        gqs
                                .getEntitiesNeedingLinking(setOf(entitySetId), configuration.loadSize)
                                .map { EntityDataKey(it.first, it.second) }
            }

            //Allow other nodes to link this entity set.
            linkingLocks.delete(entitySetId)
        }
    }


    private fun lock(candidate: EntityDataKey) {
        val existingExpiration = entityLinkingLocks.putIfAbsent(
                candidate,
                Instant.now().plusMillis(LINKING_BATCH_TIMEOUT_MILLIS).toEpochMilli()
        )
        check(existingExpiration == null) { "Unable to lock $candidate. Existing lock expires at $existingExpiration " }
    }

    private fun unlock(candidate: EntityDataKey) {
        entityLinkingLocks.delete(candidate)
    }
}

data class ScoredCluster(
        val clusterId: UUID,
        val cluster: Map<EntityDataKey, Map<EntityDataKey, Double>>,
        val score: Double
) : Comparable<Double> {
    override fun compareTo(other: Double): Int {
        return score.compareTo(other)
    }

}

private fun avgLinkCluster(matchedCluster: Map<EntityDataKey, Map<EntityDataKey, Double>>): Double {
    val clusterSize = matchedCluster.values.sumBy { it.size }
    return (matchedCluster.values.sumByDouble { it.values.sum() } / clusterSize)
}

private fun completeLinkCluster(matchedCluster: Map<EntityDataKey, Map<EntityDataKey, Double>>): Double {
    return matchedCluster.values.flatMap { it.values }.min() ?: 0.0
//    return matchedCluster.values.max { it.values.max() }.java
}