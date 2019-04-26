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
import com.google.common.collect.MapMaker
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import com.openlattice.data.EntityDataKey
import com.openlattice.data.EntityKeyIdService
import com.openlattice.edm.EntitySet
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.HazelcastQueue
import com.openlattice.linking.clustering.ClusterUpdate
import com.openlattice.postgres.mapstores.EntitySetMapstore
import com.openlattice.postgres.mapstores.EntitySetMapstore.ID_INDEX
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
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
internal const val LINKING_BATCH_TIMEOUT = 120000L
internal const val MINIMUM_SCORE = 0.75

/**
 * Performs realtime linking of individuals as they are integrated ino the system.
 */
@Component
class RealtimeLinkingService
(
        hazelcastInstance: HazelcastInstance,
        private val blocker: Blocker,
        private val matcher: Matcher,
        private val ids: EntityKeyIdService,
        private val loader: DataLoader,
        private val gqs: LinkingQueryService,
        private val executor: ListeningExecutorService,
        private val linkingFeedbackService: PostgresLinkingFeedbackService,
        private val linkableTypes: Set<UUID>,
        private val entitySetBlacklist: Set<UUID>,
        private val whitelist: Optional<Set<UUID>>,
        private val blockSize: Int
) {
    companion object {
        private val logger = LoggerFactory.getLogger(RealtimeLinkingService::class.java)
        private val clusterLocks: MutableMap<UUID, ReentrantLock> =
                MapMaker()
                        .concurrencyLevel(Runtime.getRuntime().availableProcessors() - 1)
                        .initialCapacity(1000)
                        .makeMap()
        private val clusterUpdateLock = ReentrantLock()
    }

    private val running = ReentrantLock()
    private val entitySets = hazelcastInstance.getMap<UUID, EntitySet>(HazelcastMap.ENTITY_SETS.name)
    private val linkingLocks = hazelcastInstance.getMap<UUID, Boolean>(HazelcastMap.ENTITY_SETS.name)
    private val candidates = hazelcastInstance.getQueue<EntityDataKey>(HazelcastQueue.LINKING_CANDIDATES.name)

    private val linkingWorker = executor.submit {
        Stream.generate { candidates.take() }.forEach(this::link)
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
        // if we have positive feedbacks on entity, we use its linking id and match them together
        if (hasPositiveFeedback(candidate)) {
            try {
                // only linking id of entity should remain, since we cleared neighborhood, except the ones
                // with positive feedback
                val cluster = getAndLockClusters(setOf(candidate)).entries.first()

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

                insertMatches(clusterUpdate)

            } catch (ex: Exception) {
                logger.error("An error occurred while performing linking.", ex)
                unlockClusters()
                throw IllegalStateException("Error occured while performing linking.", ex)
            }
        } else {
            // block
            val sw = Stopwatch.createStarted()
            val initialBlock = blocker.block(candidate.entitySetId, candidate.entityKeyId)
            logger.info(
                    "Blocking ($candidate.entitySetId, $candidate.entityKeyId) took ${sw.elapsed(
                            TimeUnit.MILLISECONDS
                    )} ms."
            )

            //block contains element being blocked
            val elem = initialBlock.second.getValue(candidate)

            // initialize
            sw.reset().start()
            logger.info("Initializing matching for block {}", candidate)
            val initializedBlock = matcher.initialize(initialBlock)
            logger.info("Initialization took {} ms", sw.elapsed(TimeUnit.MILLISECONDS))
            val dataKeys = collectKeys(initializedBlock.second)

            //While a best cluster is being selected and updated we can't have other clusters being updated
            try {
                val clusters = getAndLockClusters(dataKeys)

                var maybeBestCluster: ScoredCluster? = null
                var highestScore = 10.0 //Arbitrary any positive value should suffice

                clusters
                        .forEach { cluster ->
                            val scoredCluster = cluster(candidate, cluster, ::completeLinkCluster)
                            if (scoredCluster.score > MINIMUM_SCORE && (highestScore < scoredCluster.score || highestScore >= 10)) {
                                highestScore = scoredCluster.score
                                Optional
                                        .ofNullable(maybeBestCluster?.clusterId)
                                        .ifPresent { clusterId -> clusterLocks[clusterId]?.unlock() }
                                maybeBestCluster = scoredCluster
                            } else {
                                clusterLocks[cluster.key]!!.unlock()
                            }
                        }
                val clusterUpdate = if (maybeBestCluster == null) {
                    val clusterId = ids.reserveIds(LINKING_ENTITY_SET_ID, 1).first()
                    clusterLocks.getOrPut(clusterId) { ReentrantLock() }.lock()
                    val block = candidate to mapOf(candidate to elem)
                    ClusterUpdate(clusterId, candidate, matcher.match(block).second)
                } else {
                    val bestCluster = maybeBestCluster!!
                    ClusterUpdate(bestCluster.clusterId, candidate, bestCluster.cluster)
                }

                insertMatches(clusterUpdate)

            } catch (ex: Exception) {
                logger.error("An error occurred while performing linking.", ex)
                unlockClusters()
                throw IllegalStateException("Error occured while performing linking.", ex)
            }
        }
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

    private fun clearNeighborhoods(entitySetId: UUID, entityKeyIds: Stream<UUID>) {
        logger.debug("Starting neighborhood cleanup of {}", entitySetId)
        val clearedCount = entityKeyIds
                .parallel()
                .map { EntityDataKey(entitySetId, it) }
                .mapToInt {
                    val positiveFeedbacks = linkingFeedbackService.getLinkingFeedbackOnEntity(FeedbackType.Positive, it)
                            .map(EntityLinkingFeedback::entityPair)
                    gqs.deleteNeighborhood(it, positiveFeedbacks)
                }
                .sum()
        logger.debug("Cleared {} neighbors from neighborhood of {}", clearedCount, entitySetId)
    }

    private fun getAndLockClusters(
            dataKeys: Set<EntityDataKey>
    ): Map<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>> {
        logger.debug("Acquiring cluster update lock.")
        clusterUpdateLock.lock()
        logger.debug("Acquired cluster update lock.")
        val requiredClusters = gqs.getIdsOfClustersContaining(dataKeys).toList()
        logger.debug(
                "Currently held cluster locks: {}",
                clusterLocks.filter { it.value.isLocked }.map { it.key })
        logger.debug("Acquiring locks for required clusters: {}", requiredClusters)
        requiredClusters.forEach { clusterLocks.getOrPut(it) { ReentrantLock() }.lock() }
        logger.debug("Acquired locks for required clusters: {}", requiredClusters)
        clusterUpdateLock.unlock()
        logger.debug("Released cluster update lock.")

        return gqs.getClustersContaining(requiredClusters)
    }

    private fun unlockClusters() {
        if (clusterUpdateLock.isLocked) {
            clusterUpdateLock.unlock()
        }
        clusterLocks.values.forEach { lock ->
            if (lock.isLocked) {
                lock.unlock()
            }
        }
    }

    private fun insertMatches(clusterUpdate: ClusterUpdate) {
        gqs.insertMatchScores(clusterUpdate.clusterId, clusterUpdate.scores)
        gqs.updateLinkingTable(clusterUpdate.clusterId, clusterUpdate.newMember)
        clusterLocks[clusterUpdate.clusterId]!!.unlock()
    }

    @Timed
    @Scheduled(fixedRate = 30000)
    fun updateCandidateList() {
        if (!running.tryLock()) {
            return
        }
        
        try {
            val ltFilter = Predicates.`in`(EntitySetMapstore.ENTITY_TYPE_ID_INDEX, *linkableTypes.toTypedArray())
            val blFilter = Predicates.not(
                    Predicates.`in`(EntitySetMapstore.ID_INDEX, *entitySetBlacklist.toTypedArray())
            )
            val filter = whitelist.map { wl ->
                Predicates.and(ltFilter, blFilter, Predicates.`in`(ID_INDEX, *wl.toTypedArray()))
            }.orElseGet { Predicates.and(ltFilter, blFilter) }

            val linkableEntitySets = entitySets.keySet(filter)

            logger.info(
                    "Attempting to update candidates for the following linkable entity sets {}.",
                    linkableEntitySets
            )

            linkableEntitySets
                    .asSequence()
                    .filter { entitySetId ->
                        //Filter any entity sets that are currently locked for a call to entities needing linking.
                        linkingLocks.putIfAbsent(
                                entitySetId,
                                true,
                                LINKING_BATCH_TIMEOUT,
                                TimeUnit.MILLISECONDS
                        ) != true
                    }
                    .forEach { entitySetId ->
                        logger.info(
                                "Queueing entities needing linking in entity set {} ({}) .",
                                entitySets.getValue(entitySetId).name,
                                entitySetId
                        )
                        gqs.getEntitiesNeedingLinking(setOf(entitySetId))
                                .map { EntityDataKey(it.first, it.second) }
                                .forEach(candidates::put)

                        //Allow other nodes to link entity sets.
                        linkingLocks.set(entitySetId, false)
                    }

        } catch (ex: Exception) {
            logger.info("Encountered error while updating candidates for linking.")
        } finally {
            running.unlock()
        }
    }
}

data class ScoredCluster(
        val clusterId: UUID,
        val cluster: Map<EntityDataKey, Map<EntityDataKey, Double>>,
        val score: Double
)

private fun avgLinkCluster(matchedCluster: Map<EntityDataKey, Map<EntityDataKey, Double>>): Double {
    val clusterSize = matchedCluster.values.sumBy { it.size }
    return (matchedCluster.values.sumByDouble { it.values.sum() } / clusterSize)
}

private fun completeLinkCluster(matchedCluster: Map<EntityDataKey, Map<EntityDataKey, Double>>): Double {
    return matchedCluster.values.flatMap { it.values }.min() ?: 0.0
//    return matchedCluster.values.max { it.values.max() }.java
}