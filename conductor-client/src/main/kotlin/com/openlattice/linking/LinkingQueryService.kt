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
import com.openlattice.postgres.streams.BasePostgresIterable
import java.sql.Connection
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface LinkingQueryService {

    /**
     * Inserts the results scoring pairs of elements within a cluster to persistent storage. The initial cluster usually
     * consists of all scored pairs within a single block returned by the [com.openlattice.linking.Blocker].
     *
     * Commits the transaction, assuming that lockClustersForUpdates was called at some point before calling this
     *
     * @param clusterId A unique identifier for cluster that is being stored.
     * @param scores The scores pairs of elements within a cluster.
     * @return The total number of stored elements.
     */
    fun insertMatchScores(
            connection: Connection,
            clusterId: UUID,
            scores: Map<EntityDataKey, Map<EntityDataKey, Double>>): Int

    /**
     * Performs the following operations:
     *
     * - Creates a transaction with lockClustersForUpdates
     * - Invokes doWork function
     * - Commits the transaction with insertMatchScores
     * - Returns the results of doWork
     *
     * @param doWork A function that takes a set of locked clusters, performs clustering and blocking
     *  returning the final cluster as well as whether it was newly created
     * @return A triple consisting of the final cluster (first + second) and whether it was newly created (third)
     */
    fun lockClustersDoWorkAndCommit(
            candidate: EntityDataKey,
            candidates: Set<EntityDataKey>,
            doWork: ( clusters: Map<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>>  ) -> Triple<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>, Boolean>
    ): Triple<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>, Boolean>

    fun updateLinkingInformation(linkingId: UUID, newMember: EntityDataKey, cluster: Map<UUID, LinkedHashSet<UUID>>)

    fun createLinks(linkingId: UUID, toAdd: Set<EntityDataKey>): Int

    fun tombstoneLinks(linkingId: UUID, toRemove: Set<EntityDataKey>): Int

    fun getClusterFromLinkingId(linkingId: UUID): Map<EntityDataKey, Map<EntityDataKey, Double>>

    fun deleteNeighborhood(entity: EntityDataKey, positiveFeedbacks: Collection<EntityKeyPair>): Int

    fun deleteNeighborhoods(entitySetId: UUID, entityKeyIds: Set<UUID>): Int

    /**
     * Retrieve several clusters.
     * @param dataKeys The ids for the clusters to load.
     * @return The graph of scores for each cluster requested.
     */
    fun getClustersForIds(dataKeys: Set<EntityDataKey>): Map<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>>

    fun deleteEntitySetNeighborhood(entitySetId: UUID): Int

    fun updateIdsTable(clusterId: UUID, newMember: EntityDataKey): Int

    fun getEntitiesNeedingLinking(entitySetId: UUID, limit: Int = 10_000): BasePostgresIterable<EntityDataKey>

    fun getEntitiesNotLinked(entitySetIds: Set<UUID>, limit: Int = 10_000): BasePostgresIterable<Pair<UUID, UUID>>

    fun getLinkableEntitySets(
            linkableEntityTypeIds: Set<UUID>,
            entitySetBlacklist: Set<UUID>,
            whitelist: Set<UUID>
    ): BasePostgresIterable<UUID>


    /**
     * Opens a transaction, locking rows in Matched Entities table
     *  assuming that insertMatchScores will be called at some point later
     */
    fun lockClustersForUpdates(clusters: Set<UUID>): Connection

    fun getEntityKeyIdsOfLinkingIds(
            linkingIds: Set<UUID>,
            normalEntitySetIds: Set<UUID>
    ): BasePostgresIterable<Pair<UUID, Set<UUID>>>

    fun createOrUpdateLink(linkingId: UUID, cluster: Map<UUID, LinkedHashSet<UUID>>)
}


