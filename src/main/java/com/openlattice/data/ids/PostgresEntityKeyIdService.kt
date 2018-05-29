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

package com.openlattice.data.ids

import com.google.common.base.Preconditions.checkState
import com.google.common.collect.HashMultimap
import com.google.common.collect.Multimaps
import com.google.common.collect.SetMultimap
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.openlattice.data.EntityKey
import com.openlattice.data.EntityKeyIdService
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.ids.HazelcastIdGenerationService
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.ResultSetAdapters
import com.zaxxer.hikari.HikariDataSource
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
private val entityKeySql = "SELECT * FROM ${IDS.name} WHERE ${ID.name} = ? "
private val entityKeysSql = "SELECT * FROM ${IDS.name} WHERE ${ID.name} IN " +
        "(SELECT * FROM UNNEST( (?)::uuid[] )) "
private val entityKeyIdsSql = "SELECT * FROM ${IDS.name} WHERE ${ENTITY_SET_ID.name} = ? AND ${ENTITY_ID.name} IN " +
        "(SELECT * FROM UNNEST( (?)::text[] )) "
private val entityKeyIdSql = "SELECT * FROM ${IDS.name} WHERE ${ENTITY_SET_ID.name} = ? AND ${ENTITY_ID.name} =? "
private val INSERT_SQL = "INSERT INTO ${IDS.name} (${ENTITY_SET_ID.name},${ENTITY_ID.name},${ID.name})" +
        " VALUES(?,?,?)"

class PostgresEntityKeyIdService(
        hazelcastInstance: HazelcastInstance,
        private val hds: HikariDataSource,
        private val idGenerationService: HazelcastIdGenerationService
) : EntityKeyIdService {
    private val entitySetCounts: IMap<UUID, Long> = hazelcastInstance.getMap(HazelcastMap.ENTITY_SET_COUNTERS.name)

    override fun assignEntityKeyIds(entitySetId: UUID, entityIds: MutableSet<String>): MutableMap<String, UUID> {

    }

    private fun genEntityKeyIds(entityIds: SetMultimap<UUID, String>): Map<String, UUID> {
        val ids = idGenerationService.getNextIds(entityIds.size())
        checkState(ids.size != entityIds.size(), "Insufficient ids generated.")

        val idIterator = ids.iterator()
        return entityIds.values().map { it to idIterator.next() }.toMap()
    }

    private fun storeEntityKeyIds(entitySetId: UUID) {

    }


    override fun reserveIds(entitySetId: UUID?, count: Int): MutableList<UUID> {
        val base = entitySetCounts.executeOnKey(entitySetId, IdReservingEntryProcessor(count)) as Long
        val ids = idGenerationService.getNextIds(count)

    }

    override fun getEntityKeyId(entitySetId: UUID?, entityId: String?): UUID? {
        return loadEntityKeyId(entitySetId, entityId) ?: assignEntityKeyIds(entitySetId, setOf(entityId))

    }

    override fun getEntityKeyId(entityKey: EntityKey): UUID {
        return getEntityKeyId(entityKey.entitySetId, entityKey.entityId)
    }

    private fun loadEntityKeyId(entitySetId: UUID?, entityId: String?): UUID? {
        val connection = hds.getConnection()
        val ps = connection.prepareStatement(entityKeyIdSql)
        ps.setObject(1, entitySetId)
        ps.setString(2, entityId)
        val rs = ps.executeQuery()

        return if (rs.next()) {
            ResultSetAdapters.id(rs)
        } else {
            null
        }
    }

    private fun loadEntityKeyIds(entityIds: SetMultimap<UUID, String>): Map<EntityKey, UUID> {
        val connection = hds.getConnection()
        val ids = HashMap<EntityKey, UUID>(entityIds.size())

        Multimaps
                .asMap(entityIds)
                .forEach {
                    val ps = connection.prepareStatement(entityKeyIdsSql)
                    ps.setObject(1, it.key)
                    ps.setArray(2, PostgresArrays.createTextArray(connection, it.value))
                    val rs = ps.executeQuery()
                    while (rs.next()) {
                        ids.put(ResultSetAdapters.entityKey(rs), ResultSetAdapters.id(rs))
                    }
                }

        return ids
    }

    override fun getEntityKeyIds(entityKeys: Set<EntityKey>): Map<EntityKey, UUID> {
        val entityIds: SetMultimap<UUID, String> = HashMultimap.create()
        entityKeys.forEach { entityIds.put(it.entitySetId, it.entityId) }

        return loadEntityKeyIds(entityIds)
    }

    override fun getEntityKeyEntries(entityKeyIds: Set<UUID>): Set<Map.Entry<EntityKey, UUID>> {
        val connection = hds.getConnection()
        val ps = connection.prepareStatement(entityKeysSql)
        val entries = HashMap<EntityKey, UUID>(entityKeyIds.size)

        ps.setArray(1, PostgresArrays.createUuidArray(connection, entityKeyIds))
        val rs = ps.executeQuery()

        while (rs.next()) {
            entries.put(ResultSetAdapters.entityKey(rs), ResultSetAdapters.id(rs))
        }
        return entries.entries
    }

    override fun getEntityKey(entityKeyId: UUID): EntityKey? {
        val connection = hds.getConnection()
        val ps = connection.prepareStatement(entityKeySql)
        ps.setObject(1, entityKeyId)
        val rs = ps.executeQuery()
        return if (rs.next()) {
            ResultSetAdapters.entityKey(rs)
        } else {
            null
        }
    }

    override fun getEntityKeys(entityKeyIds: Set<UUID>): Map<UUID, EntityKey> {
        val connection = hds.getConnection()
        val ps = connection.prepareStatement(entityKeysSql)
        val entries = HashMap<UUID, EntityKey>(entityKeyIds.size)

        ps.setArray(1, PostgresArrays.createUuidArray(connection, entityKeyIds))
        val rs = ps.executeQuery()

        while (rs.next()) {
            entries.put(ResultSetAdapters.id(rs), ResultSetAdapters.entityKey(rs))
        }

        return entries
    }

}