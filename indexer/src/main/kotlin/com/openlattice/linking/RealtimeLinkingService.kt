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

import com.openlattice.data.EntityDataKey
import com.openlattice.data.EntityKeyIdService
import com.openlattice.postgres.streams.PostgresIterable
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Stream
import kotlin.streams.asSequence


/**
 * Entity sets ids are assigned by calling [UUID.randomUUID] as a result we know that this can never be accidentally
 * assigned to any real entity set.
 */
internal val LINKING_ENTITY_SET_ID = UUID(0, 0)
internal const val PERSON_FQN = "general.person"
internal const val REFRESH_PROPERTY_TYPES_INTERVAL_MILLIS = 10000L
/**
 *
 * Performs realtime linking of individuals as they are integrated ino the system.
 */
class RealtimeLinkingService
(
        val blocker: Blocker,
        private val matcher: Matcher,
        private val clusterer: Clusterer,
        private val ids: EntityKeyIdService,
        private val gqs: LinkingQueryService

) {
    companion object {
        private val logger = LoggerFactory.getLogger(RealtimeLinkingService::class.java)
    }

    /**
     * Linking:
     * 1) For each new person entity perform blocking and identify candidate clusters
     */
    /**
     * Performs an update of the existing links for recently written data. The main challenge here is clustering
     * after matching. Our approach is to
     */
    private fun blockAndInitializeMatching(
            entitySetId: UUID,
            entityKeyIds: Stream<UUID>,
            count: Optional<Int> = Optional.empty()
    ) {
        val reservedIds = count.map { ids.reserveIds(LINKING_ENTITY_SET_ID, it) }
        val index = AtomicInteger()
        /*
         * Perform an update of matching scores.
         *
         * 1)  We initialize a cluster from each block
         *
         * 2)
         */
        entityKeyIds
                .parallel()
                .map { blocker.block(entitySetId, it) }
                .peek {
                    val initializedBlock = matcher.initialize(it)
                    gqs.insertMatchScores(
                            reservedIds
                                    .map { it[index.getAndIncrement()] }
                                    .orElseGet { ids.reserveIds(LINKING_ENTITY_SET_ID, 1)[0] },
                            initializedBlock.second
                    )
                }
                .map(matcher::initialize)

    }


    private fun updateLinks(
            entitySetId: UUID,
            entityKeyIds: Iterable<UUID>,
            count: Optional<Int> = Optional.empty()
    ) {

        entityKeyIds
                .asSequence()
                .map { blocker.block(entitySetId, it ) }
                .map {
                    //Remember initialized block contains itself.
        val initializedBlock = matcher.initialize(it)
                    clusterer.getCandidateClusters(it.second.keys)
        gqs.insertMatchScores(
                reservedIds
                        .map { it[index.getAndIncrement()] }
                        .orElseGet { ids.reserveIds(LINKING_ENTITY_SET_ID, 1)[0] },
                initializedBlock.second
        ) }
                .map ( clusterer::getCandidateClusters )
                .map ( gqs.load)
                .map ( matcher::scoreBestCluster )
                .forEach ( clusterer::addToCluster )


    }

    private fun clearNeighborhood(entitySetId: UUID, entityKeyIds: Stream<UUID>) {
        logger.debug("Starting neighborhood cleanup of {}", entitySetId)
        val clearedCount = entityKeyIds
                .parallel()
                .map { EntityDataKey(entitySetId, it) }
                .mapToInt(gqs::deleteNeighborhood)
                .sum()
        logger.debug("Cleared {} neighbors from neighborhood of {}", clearedCount, entitySetId)
    }

    fun refreshLinks(entitySetId: UUID, entityKeyIds: PostgresIterable<UUID>) {
        clearNeighborhood(entitySetId, entityKeyIds.stream())
        blockAndInitializeMatching(entitySetId, entityKeyIds.stream())
        clusterer.cluster(0.25)
    }


    fun refreshLinks(entitySetId: UUID, entityKeyIds: Collection<UUID>) {
        clearNeighborhood(entitySetId, entityKeyIds.stream())
        blockAndInitializeMatching(entitySetId, entityKeyIds.stream(), Optional.of(entityKeyIds.size()))
    }

    fun delete(entitySetId: UUID, entityKeyIds: Set<UUID>) {

    }

    fun updateModel(serializedModel: ByteArray) {

    }
}