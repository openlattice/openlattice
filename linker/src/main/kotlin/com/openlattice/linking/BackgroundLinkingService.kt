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

import com.codahale.metrics.Histogram
import com.codahale.metrics.Meter
import com.codahale.metrics.MetricRegistry
import com.google.common.base.Stopwatch
import com.google.common.collect.Sets
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import com.openlattice.data.EntityDataKey
import com.openlattice.data.EntityKeyIdService
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.HazelcastQueue
import com.openlattice.linking.blocking.Block
import com.openlattice.linking.blocking.Blocker
import com.openlattice.linking.clustering.Cluster
import com.openlattice.linking.clustering.Clusterer
import com.openlattice.linking.clustering.KeyedCluster
import com.openlattice.linking.matching.Matcher
import com.openlattice.postgres.mapstores.EntitySetMapstore
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Instant
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

internal const val REFRESH_PROPERTY_TYPES_INTERVAL_MILLIS = 30000L
internal const val LINKING_BATCH_TIMEOUT_MILLIS = 120000L
internal const val MINIMUM_SCORE = 0.75
internal const val LINKING_RATE = 300_000L

/**
 * Performs realtime linking of individuals as they are integrated ino the system.
 */
@Component
class BackgroundLinkingService(
        private val executor: ListeningExecutorService,
        hazelcastInstance: HazelcastInstance,
        private val blocker: Blocker,
        private val matcher: Matcher,
        private val ids: EntityKeyIdService,
        private val clusterer: Clusterer,
        private val lqs: LinkingQueryService,
        private val linkingFeedbackService: PostgresLinkingFeedbackService,
        private val linkableTypes: Set<UUID>,
        private val configuration: LinkingConfiguration
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundLinkingService::class.java)

        private val metrics: MetricRegistry = MetricRegistry()
        val featureExtraction: Histogram = metrics.histogram("feature-extraction")
        private val requests: Meter = metrics.meter("links")
    }

    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)
    private val linkingLocks = HazelcastMap.LINKING_LOCKS.getMap(hazelcastInstance)
    private val candidates = HazelcastQueue.LINKING_CANDIDATES.getQueue( hazelcastInstance )
//    private val priorityEntitySets = configuration.whitelist.orElseGet { setOf() }

    @Suppress("UNUSED")
    @Scheduled(fixedRate = LINKING_RATE)
    fun enqueue() {
        val linkedInSession = requests.count
        val currentHourlyRate = requests.fifteenMinuteRate * 60 * 60
        val timeLeft = candidates.size / currentHourlyRate
        logger.info("$linkedInSession entities linked since last startup. That's a rate of $currentHourlyRate per hour. The current queue will be run through in $timeLeft hours")
        if ( candidates.isNotEmpty() ){
            logger.info("Linking queue still has candidates on it, not adding more at the moment")
            return
        }
        try {
            val filteredLinkableEntitySetIds = entitySets.keySet(
                    Predicates.and(
                            Predicates.`in`<UUID, EntitySet>(EntitySetMapstore.ENTITY_TYPE_ID_INDEX, *linkableTypes.toTypedArray()),
                            Predicates.notEqual<UUID,EntitySet>(EntitySetMapstore.FLAGS_INDEX, EntitySetFlag.LINKING)
                    )
            )

//            val rest = filteredLinkableEntitySetIds.asSequence().filter {
//                !priorityEntitySets.contains(it)
//            }

//            val priority = priorityEntitySets.asSequence().filter {
//                filteredLinkableEntitySetIds.contains(it)
//            }
            listOf(
                    UUID.fromString("2deb5292-11b5-4874-b4b1-2a5a57804e68"),
                    UUID.fromString("e9dc56bf-7cf9-4e25-8969-1ac4c1b1e1ec"),
                    UUID.fromString("9d9a6c3e-fd82-4599-9dcc-a2602e2bd54d"),
//                    UUID.fromString("9d9a6c3e-fd82-4599-9dcc-a2602e2bd54d"),
                    UUID.fromString("4e369747-66aa-432f-8aa3-2591cee0fa8d")
            ).filter {
                val es = entitySets[it]
                if ( es == null ){
                    logger.info("Entityset with id {} doesnt exist", it)
                    return@filter false
                }

                val keep = filteredLinkableEntitySetIds.contains(it)
                if ( keep ) {
                    logger.info("including entityset {} for candidate linking", es.id )
                } else {
                    logger.info("excluding entityset {} because its not linkable", es.name)
                }
                keep
            }.forEach { esid ->
                val es = entitySets.getValue(esid)
                logger.info("Starting to queue linking candidates from entity set {}({})", es.name, esid)
                val forLinking = lqs.getEntitiesNeedingLinking(
                        esid,
                        3 * configuration.loadSize
                )
//                        ).filter {
//                    if( tryLockCandidate(it) ) {
//                        logger.info("successfully locked $it for linking")
//                        true
//                    } else {
//                        logger.info("$it already locked for linking")
//                        false
//                    }
//                    val expiration = refreshExpiration(it)
//                    logger.info("Considering candidate {} with expiration {} at {}", it, expiration, now())
//                    if (expiration != null && now() >= expiration) {
//                        logger.info("Refreshing expiration for edk {}", it)
//                        Assume original lock holder died, probably somewhat unsafe
//                        refreshExpiration(it)
//                        true
//                    } else {
//                        expiration == null
//                    }
//                }

//                logger.info("Entities needing linking")
                logger.debug("Entities needing linking: {}", forLinking)
                candidates.addAll(forLinking)
                logger.info( "Queued entities needing linking")
            }
        } catch (ex: Exception) {
            logger.info("Encountered error while updating candidates for linking.", ex)
        }
    }

    private val limiter = Semaphore(configuration.parallelism * 2)

    @Suppress("UNUSED")
    private val linkingWorker = if (isLinkingEnabled()) executor.submit {
        while (true) {
            try {
                val candidate = candidates.take()
                limiter.acquire()
                executor.submit( Runnable {
                    if( !tryLockCandidate(candidate) ) {
                        logger.info("candidate already locked for linking: {}\nNot resubmitting", candidate)
//                        refreshExpiration(it)
                        return@Runnable
                    }
                    logger.info("candidate freshly locked for linking: {}", candidate)
                    try {
                        logger.info("Linking {}", candidate)
                        link(candidate)
                        logger.info("Finished linking {}", candidate)
                    } catch (ex: Exception) {
                        logger.error("Unable to link {}.", candidate, ex)
                    } finally {
                        logger.info("Unlocking candidate after linking: {}", candidate)
                        unlock(candidate)
                        requests.mark()
                        limiter.release()
                    }
                })
            } catch (ex: Exception) {
                logger.info("Encountered error while linking candidates.", ex)
            }
        }
    } else null

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
//        if (linkingFeedbackService.hasFeedbacks(FeedbackType.Positive, candidate)) {
//            try {
//                // only linking id of entity should remain, since we cleared neighborhood, except the ones
//                // with positive feedback
//                val clusters = Clusters(lqs.getClustersForIds(setOf(candidate)))
//                val cluster = KeyedCluster.fromEntry(clusters.entries.first())
//                val clusterId = cluster.id
//                lateinit var scoredCluster: ScoredCluster
//
//                lqs.lockClustersForUpdates(setOf(clusterId)).use { conn ->
//                    scoredCluster = clusterer.cluster(candidate, cluster, ::completeLinkCluster)
//                    if (scoredCluster.score <= MINIMUM_SCORE) {
//                        logger.error(
//                                "Recalculated score {} of linking id {} with positives feedbacks did not pass minimum score {}",
//                                scoredCluster.score,
//                                cluster.id,
//                                MINIMUM_SCORE
//                        )
//                    }
//                    lqs.insertMatchScores(conn, clusterId, scoredCluster.cluster)
//                }
//                insertMatches(clusterId, candidate, scoredCluster.cluster)
//            } catch (ex: Exception) {
//                logger.error("An error occurred while performing linking.", ex)
//                throw IllegalStateException("Error occured while performing linking.", ex)
//            }
//        } else {
            // Run standard blocking + clustering
            val sw = Stopwatch.createStarted()
            val initialBlock = blocker.block(candidate.entitySetId, candidate.entityKeyId)

            logger.info(
                    "Blocking ({}, {}) took {} ms.",
                    candidate.entitySetId,
                    candidate.entityKeyId,
                    sw.elapsed(TimeUnit.MILLISECONDS)
            )

            //block contains element being blocked
            val elem = initialBlock.entities.getValue(candidate)

            // initialize
            sw.reset().start()
            logger.info("Initializing matching for block {}", candidate)
            val initializedBlock = matcher.initialize(initialBlock)
            logger.info("Initialization took {} ms", sw.elapsed(TimeUnit.MILLISECONDS))
            val dataKeys = collectKeys(initializedBlock.matches)

            //Decision that needs to be made is whether to start new cluster or merge into existing cluster.
            //No locks are required since any items that block to this element will be skipped.
            try {
                val (linkingId, scores) = lqs.lockClustersDoWorkAndCommit( candidate, dataKeys) { clusters ->
                    val maybeBestCluster = clusters
                            .asSequence()
                            .map { cluster -> clusterer.cluster(candidate, KeyedCluster.fromEntry(cluster), ::completeLinkCluster) }
                            .filter { it.score > MINIMUM_SCORE }
                            .maxBy { it.score }

                    if ( maybeBestCluster != null ) {
                        return@lockClustersDoWorkAndCommit Triple(maybeBestCluster.clusterId, maybeBestCluster.cluster, false)
                    }
                    val linkingId = ids.reserveLinkingIds(1).first()
                    val block = Block(candidate, mapOf(candidate to elem))
                    val cluster = matcher.match(block).matches
                    //TODO: When creating new cluster do we really need to re-match or can we assume score of 1.0?
                    return@lockClustersDoWorkAndCommit Triple(linkingId, cluster, true)
                }
                insertMatches( linkingId, candidate, Cluster(scores) )
            } catch (ex: Exception) {
                logger.error("An error occurred while performing linking.", ex)
                throw IllegalStateException("Error occured while performing linking.", ex)
            }
//        }
    }

    private fun <T> collectKeys(m: Map<EntityDataKey, Map<EntityDataKey, T>>): Set<EntityDataKey> {
        return m.keys + m.values.flatMap { it.keys }
    }

    private fun clearNeighborhoods(candidate: EntityDataKey) {
        logger.debug("Starting neighborhood cleanup of {}", candidate)
        val positiveFeedbacks = listOf<EntityKeyPair>()
//                linkingFeedbackService.getLinkingFeedbackEntityKeyPairs(
//                FeedbackType.Positive, candidate
//        )

        val clearedCount = lqs.deleteNeighborhood(candidate, positiveFeedbacks)
        logger.debug("Cleared {} neighbors from neighborhood of {}", clearedCount, candidate)
    }

    private fun insertMatches(
            linkingId: UUID,
            newMember: EntityDataKey,
            scores: Cluster
    ) {
        val scoresAsEsidToEkids = (collectKeys(scores) + newMember)
                .groupBy { edk -> edk.entitySetId }
                .mapValues { (_, edks) ->

                    Sets.newLinkedHashSet(edks.map { it.entityKeyId })
                }
        lqs.updateLinkingInformation( linkingId, newMember, scoresAsEsidToEkids )
    }

    /**
     * @return Null if locked, expiration in millis otherwise.
     */
//    private fun refreshExpiration(candidate: EntityDataKey): Long? {
//        return try {
//            linkingLocks.lock(candidate)
//
//            tryLockCandidateRaw(candidate)
//        } finally {
//            linkingLocks.unlock(candidate)
//        }
//    }

    private fun isLinkingEnabled(): Boolean {
        if (!configuration.backgroundLinkingEnabled) {
            logger.info("Skipping task as background linking is not enabled.")
            return false
        }

        return true
    }

    /**
     * Locks the candidate if possible
     *
     * Returns true if the candidate has been successfully locked, false if already locked
     */
    private fun tryLockCandidate(candidate: EntityDataKey): Boolean {
        return tryLockCandidateRaw(candidate) == null
    }

    private fun tryLockCandidateRaw(candidate: EntityDataKey): Long? {
        return linkingLocks.putIfAbsent(
                candidate,
                Instant.now().plusMillis(LINKING_BATCH_TIMEOUT_MILLIS).toEpochMilli(),
                LINKING_BATCH_TIMEOUT_MILLIS,
                TimeUnit.MILLISECONDS
        )
    }

    private fun unlock(candidate: EntityDataKey) {
        linkingLocks.delete(candidate)
    }

    private fun completeLinkCluster( matchedCluster: Cluster ): Double {
        return matchedCluster.values.flatMap { it.values }.min() ?: 0.0
    }
}
