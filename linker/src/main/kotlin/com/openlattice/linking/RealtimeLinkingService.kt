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
import com.hazelcast.core.HazelcastInstance
import com.openlattice.data.EntityDataKey
import com.openlattice.data.EntityKeyIdService
import com.openlattice.edm.EntitySet
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.linking.clustering.ClusterUpdate
import com.openlattice.postgres.streams.PostgresIterable
import org.deeplearning4j.clustering.strategy.BaseClusteringStrategy
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
        private val linkableTypes: Set<UUID>,
        private val blockSize: Int
) {
    companion object {
        private val logger = LoggerFactory.getLogger(RealtimeLinkingService::class.java)
        private val clusterLocks: MutableMap<UUID, ReentrantLock> = mutableMapOf()
        private val clusterUpdateLock = ReentrantLock()
    }

    private val running = ReentrantLock()
    private val entitySets = hazelcastInstance.getMap<UUID, EntitySet>(HazelcastMap.ENTITY_SETS.name)
    /**
     * Linking:
     * 1) For each new person entity perform blocking
     * 2) Use the results of block to identify candidate clusters
     * 3) Insert the results of the match scores
     * 4) Update the linked entities table.
     */
    private fun runIterativeLinking(
            entitySetId: UUID,
            entityKeyIds: Stream<UUID>
    ) {

        entityKeyIds
                .parallel()
                .map {
                    val sw = Stopwatch.createStarted()
                    val block = blocker.block(entitySetId, it)
                    logger.info("Blocking ($entitySetId, $it) took ${sw.elapsed(TimeUnit.MILLISECONDS)} ms.")
                    block
                }
                .filter {
                    if (it.second.containsKey(it.first)) {
                        return@filter true
                    } else {
                        logger.error("Skipping block for data key: {}", it.first)
                    }

                    false
                }
                .forEach {
                    //block contains element being blocked
                    val blockKey = it.first
                    val elem = it.second[blockKey]!!
                    val initializedBlock = matcher.initialize(it)
                    val dataKeys = collectKeys(initializedBlock.second)
                    //While a best cluster is being selected and updated we can't have other clusters being updated

                    try {
                        logger.info("Acquiring cluster update lock.")
                        clusterUpdateLock.lock()
                        logger.info("Acquired cluster update lock.")
                        val requiredClusters = gqs.getIdsOfClustersContaining(dataKeys).toList()
                        logger.info(
                                "Currently held cluster locks: {}",
                                clusterLocks.filter { it.value.isLocked }.map { it.key })
                        logger.info("Acquiring locks for required clusters: {}", requiredClusters)
                        requiredClusters.forEach { clusterLocks.getOrPut(it) { ReentrantLock() }.lock() }
                        logger.info("Acquired locks for required clusters: {}", requiredClusters)
                        clusterUpdateLock.unlock()
                        logger.info("Released cluster update lock.")

                        val clusters = gqs.getClustersContaining(requiredClusters)


                        var maybeBestCluster: ScoredCluster? = null
                        var highestScore = 10.0 //Arbitrary any positive value should suffice

                        clusters
                                .forEach {
                                    val scoredCluster = cluster(blockKey, it, ::completeLinkCluster)
                                    if (scoredCluster.score > MINIMUM_SCORE && (highestScore > scoredCluster.score || highestScore >= 10)) {
                                        highestScore = scoredCluster.score
                                        Optional
                                                .ofNullable(maybeBestCluster?.clusterId)
                                                .ifPresent { clusterLocks[it]?.unlock() }
                                        maybeBestCluster = scoredCluster
                                    } else {
                                        clusterLocks[it.key]!!.unlock()
                                    }
                                }
                        val clusterUpdate = if (maybeBestCluster == null) {
                            val clusterId = ids.reserveIds(LINKING_ENTITY_SET_ID, 1).first()
                            clusterLocks.getOrPut(clusterId) { ReentrantLock() }.lock()
                            val block = blockKey to mapOf(blockKey to elem)
                            ClusterUpdate(clusterId, blockKey, matcher.match(block).second)
                        } else {
                            val bestCluster = maybeBestCluster!!
                            ClusterUpdate(bestCluster.clusterId, blockKey, bestCluster.cluster)
                        }

                        gqs.insertMatchScores(clusterUpdate.clusterId, clusterUpdate.scores)
                        gqs.updateLinkingTable(clusterUpdate.clusterId, clusterUpdate.newMember)
                        clusterLocks[clusterUpdate.clusterId]!!.unlock()

                    } catch (ex: Exception) {
                        logger.error("An error occurred while performing linking.", ex)
                        if (clusterUpdateLock.isLocked) {
                            clusterUpdateLock.unlock()
                        }
                        clusterLocks.values.forEach { lock ->
                            if (lock.isLocked) {
                                lock.unlock()
                            }
                        }
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
        return m!!.keys + m.values.flatMap { it.keys }
    }

    private fun clearNeighborhoods(entitySetId: UUID, entityKeyIds: Stream<UUID>) {
        logger.debug("Starting neighborhood cleanup of {}", entitySetId)
        val clearedCount = entityKeyIds
                .parallel()
                .map { EntityDataKey(entitySetId, it) }
                .mapToInt(gqs::deleteNeighborhood)
                .sum()
        logger.debug("Cleared {} neighbors from neighborhood of {}", clearedCount, entitySetId)
    }

    @Timed
    @Scheduled(fixedRate = 30000)
    fun runLinking() {
        logger.info("Trying to start linking job.")
        if (running.tryLock()) {
            try {
                //TODO: Make this more efficient than pulling the entire list of entity sets locally.
                //For example use a fast aggregator or directly query postgres and partition operation of linking
                val linkableEntitySets = entitySets
                        .values
                        .filter { linkableTypes.contains(it.entityTypeId) }
                        .map(EntitySet::getId)
                        .toSet()


                logger.info("Running linking using the following linkable entity sets {}.", linkableEntitySets)

                var entitiesNeedingLinking = gqs
                        .getEntitiesNeedingLinking(linkableEntitySets)
                        .groupBy({ it.first }) { it.second }


                while (entitiesNeedingLinking.isNotEmpty()) {
                    val sw = Stopwatch.createStarted()
                    entitiesNeedingLinking.forEach { entitySetId, ids -> refreshLinks(entitySetId, ids) }

                    entitiesNeedingLinking = gqs
                            .getEntitiesNeedingLinking(linkableEntitySets)
                            .groupBy({ it.first }) { it.second }
                    logger.info(
                            "Linked {} entities in {} ms", entitiesNeedingLinking.size,
                            sw.elapsed(TimeUnit.MILLISECONDS)
                    )
                    clusterLocks.clear()
                    logger.info("Cleared {} cluster locks.", clusterLocks.size)
                }
            } catch (ex: Exception) {
                logger.info("Encountered error while linking!", ex)
            } finally {
                running.unlock()
            }
        } else {
            logger.info("Linking is currently running. Not starting new task.")
        }
    }


    private fun refreshLinks(entitySetId: UUID, entityKeyIds: PostgresIterable<UUID>) {
        clearNeighborhoods(entitySetId, entityKeyIds.stream())
        runIterativeLinking(entitySetId, entityKeyIds.stream())
    }

    private fun refreshLinks(entitySetId: UUID, entityKeyIds: Collection<UUID>) {
        clearNeighborhoods(entitySetId, entityKeyIds.stream())
        runIterativeLinking(entitySetId, entityKeyIds.parallelStream())

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