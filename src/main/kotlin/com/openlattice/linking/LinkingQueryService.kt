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
import com.openlattice.postgres.streams.PostgresIterable
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface LinkingQueryService {
    /**
     * Inserts the results scoring pairs of elements within a cluster to persistent storage. The initial cluster usually
     * consists of all scored pairs within a single block returned by the [Blocker].
     *
     * @param clusterId A unique identifier for cluster that is being stored.
     * @param scores The scores pairs of elements within a cluster.
     * @return The total number of stored elements.
     */
    fun insertMatchScores(clusterId: UUID, scores: Map<EntityDataKey, Map<EntityDataKey, Double>>): Int

    fun insertMatchScore(clusterId: UUID, blockKey: EntityDataKey, blockElement: EntityDataKey, score: Double): Int

    fun getNeighborhoodScores(blockKey: EntityDataKey): Map<EntityDataKey, Double>
    fun deleteMatchScore(blockKey: EntityDataKey, blockElement: EntityDataKey): Int

    fun getOrderedBlocks(): PostgresIterable<Pair<EntityDataKey, Long>>
    fun getClustersBySize(): PostgresIterable<Pair<EntityDataKey, Double>>
    fun deleteNeighborhood(u: EntityDataKey): Int
    fun deleteNeighborhoods(entitySetId: UUID, entityKeyIds: Set<UUID>): Int
    fun getClustersContaining(clusterIds: Collection<UUID>): Map<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>>
    fun updateLinkingTable(clusterId: UUID, newMember: EntityDataKey): Int

    fun getEntitiesNeedingLinking(entitySetIds: Set<UUID>, limit: Int = 10000): PostgresIterable<Pair<UUID, UUID>>
    fun getEntitiesNotLinked(entitySetIds: Set<UUID>, limit: Int = 10000): PostgresIterable<Pair<UUID, UUID>>
    fun getLinkableEntitySets(linkableEntityTypeIds: Set<UUID>, entitySetBlacklist: Set<UUID>, whitelist: Set<UUID>): PostgresIterable<UUID>
    fun getIdsOfClustersContaining(dataKeys: Set<EntityDataKey>): PostgresIterable<UUID>
}


