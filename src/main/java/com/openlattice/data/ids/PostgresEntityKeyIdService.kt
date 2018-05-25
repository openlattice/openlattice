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

import com.google.common.collect.HashMultimap
import com.google.common.collect.Multimaps
import com.google.common.collect.SetMultimap
import com.openlattice.data.EntityKey
import com.openlattice.data.EntityKeyIdService
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
private val entityKeySql = "SELECT * FROM $IDS.name WHERE ${ID.name} = ? "
private val entityKeysSql = "SELECT * FROM $IDS.name WHERE ${ID.name} IN " +
        "(SELECT * FROM UNNEST( (?)::uuid[] )) "
private val entityKeyIdsSql = "SELECT * FROM $IDS.name WHERE ${ENTITY_SET_ID.name} = ? AND ${ENTITY_ID.name} IN " +
        "(SELECT * FROM UNNEST( (?)::text[] )) "
private val entityKeyIdSql = "SELECT * FROM $IDS.name WHERE ${ENTITY_SET_ID.name} = ? AND ${ENTITY_ID.name} =? "

class PostgresEntityKeyIdService(private val hds: HikariDataSource) : EntityKeyIdService {
    override fun getEntityKeyId(entitySetId: UUID?, entityId: String?): UUID {
        val connection = hds.getConnection()
        val ps = connection.prepareStatement(entityKeyIdSql)
        ps.setObject(1, entitySetId)
        ps.setString(2, entityId)
        return ResultSetAdapters.id(ps.executeQuery())
    }

    override fun getEntityKeyId(entityKey: EntityKey): UUID {
        return getEntityKeyId(entityKey.entitySetId, entityKey.entityId)
    }

    override fun getEntityKeyIds(entityKeys: Set<EntityKey>): Map<EntityKey, UUID> {
        val entityIds: SetMultimap<UUID, String> = HashMultimap.create()
        entityKeys.forEach { entityIds.put(it.entitySetId, it.entityId) }

        val connection = hds.getConnection()
        val ids = HashMap<EntityKey, UUID>(entityKeys.size)
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