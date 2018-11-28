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
import com.openlattice.data.EntityKey
import com.openlattice.data.EntityKeyIdService
import com.openlattice.ids.HazelcastIdGenerationService
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.ResultSetAdapters
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.util.*
import java.util.stream.Collectors
import kotlin.collections.HashMap

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
private val entityKeysSql = "SELECT * FROM ${IDS.name} WHERE ${ID.name} IN " +
        "(SELECT * FROM UNNEST( (?)::uuid[] )) "
private val entityKeyIdsSql = "SELECT * FROM ${IDS.name} WHERE ${ENTITY_SET_ID.name} = ? AND ${ENTITY_ID.name} IN " +
        "(SELECT * FROM UNNEST( (?)::text[] )) "
private val entityKeyIdSql = "SELECT * FROM ${IDS.name} WHERE ${ENTITY_SET_ID.name} = ? AND ${ENTITY_ID.name} =? "
private val INSERT_SQL = "INSERT INTO ${IDS.name} (${ENTITY_SET_ID.name},${ENTITY_ID.name},${ID.name})" +
        " VALUES(?,?,?)"
private val logger = LoggerFactory.getLogger(PostgresEntityKeyIdService::class.java)

class PostgresEntityKeyIdService(
        hazelcastInstance: HazelcastInstance,
        private val hds: HikariDataSource,
        private val idGenerationService: HazelcastIdGenerationService
) : EntityKeyIdService {

    private fun genEntityKeyIds(entityIds: Set<EntityKey>): Map<EntityKey, UUID> {
        val ids = idGenerationService.getNextIds(entityIds.size)
        checkState(ids.size == entityIds.size, "Insufficient ids generated.")

        val idIterator = ids.iterator()
        return entityIds.map { it to idIterator.next() }.toMap()
    }

    private fun storeEntityKeyIds(entityKeyIds: Map<EntityKey, UUID>): Map<EntityKey, UUID> {
        val connection = hds.connection
        connection.use {
            val ps = connection.prepareStatement(INSERT_SQL)
            entityKeyIds.forEach {
                ps.setObject(1, it.key.entitySetId)
                ps.setString(2, it.key.entityId)
                ps.setObject(3, it.value)
                ps.addBatch()
            }
            val totalWritten = ps.executeBatch().sum()
            if (totalWritten != entityKeyIds.size) {
                logger.warn("Expected ${entityKeyIds.size} entity key writes. Only $totalWritten writes registered.")
            }
        }
        return entityKeyIds
    }


    override fun reserveIds(entitySetId: UUID, count: Int): List<UUID> {
        val ids = idGenerationService.getNextIds(count)
        storeEntityKeyIds(ids.map { EntityKey(entitySetId, it.toString()) to it }.toMap())
        return ids.toList()
    }

    override fun getEntityKeyId(entitySetId: UUID, entityId: String): UUID {
        val entityKey = EntityKey(entitySetId, entityId)
        return getEntityKeyId(entityKey)

    }

    override fun getEntityKeyId(entityKey: EntityKey): UUID {
        return loadEntityKeyId(entityKey.entitySetId, entityKey.entityId) ?: storeEntityKeyIds(
                genEntityKeyIds(setOf(entityKey))
        )[entityKey]!!
    }

    private fun loadEntityKeyId(entitySetId: UUID, entityId: String): UUID? {
        return hds.connection.use {
            val ps = it.prepareStatement(entityKeyIdSql)
            ps.setObject(1, entitySetId)
            ps.setString(2, entityId)
            val rs = ps.executeQuery()

            return if (rs.next()) {
                ResultSetAdapters.id(rs)
            } else {
                null
            }
        }
    }

    private fun loadEntityKeyIds(entityIds: SetMultimap<UUID, String>): MutableMap<EntityKey, UUID> {
        return hds.connection.use {
            val connection = it
            val ids = HashMap<EntityKey, UUID>(entityIds.size())

            Multimaps
                    .asMap(entityIds)
                    .forEach {
                        val ps = connection.prepareStatement(entityKeyIdsSql)
                        ps.setObject(1, it.key)
                        ps.setArray(2, PostgresArrays.createTextArray(connection, it.value))
                        val rs = ps.executeQuery()
                        while (rs.next()) {
                            ids[ResultSetAdapters.entityKey(rs)] = ResultSetAdapters.id(rs)
                        }
                    }

            return ids
        }
    }
    override fun getEntityKeyIds(
            entityKeys: Set<EntityKey>, entityKeyIds: MutableMap<EntityKey, UUID>
    ): MutableMap<EntityKey, UUID> {
        val entityIds: SetMultimap<UUID, String> = HashMultimap.create()
        entityKeys.forEach { entityIds.put(it.entitySetId, it.entityId) }
        entityKeyIds.putAll(loadEntityKeyIds(entityIds))

        //Making this line O(n) is why we chose to just take a set instead of a sequence (thus allowing lazy views since copy is required anyway)
        val missing = entityKeys.minus(entityKeyIds.keys)
        entityKeyIds.putAll(storeEntityKeyIds(genEntityKeyIds(missing)))

        return entityKeyIds
    }

    override fun getEntityKeyIds(entityKeys: Set<EntityKey>): MutableMap<EntityKey, UUID> {
        return getEntityKeyIds(entityKeys, HashMap(entityKeys.size))
    }

    override fun getEntityKey(entityKeyId: UUID): EntityKey {
        return loadEntityKeys(setOf(entityKeyId))[entityKeyId]!!
    }

    override fun getEntityKeys(entityKeyIds: Set<UUID>): MutableMap<UUID, EntityKey> {
        return loadEntityKeys(entityKeyIds)
    }

    private fun loadEntityKeys(entityKeyIds: Set<UUID>): MutableMap<UUID, EntityKey> {
        val connection = hds.connection
        val entries = HashMap<UUID, EntityKey>(entityKeyIds.size)

        connection.use {
            val ps = connection.prepareStatement(entityKeysSql)

            ps.setArray(1, PostgresArrays.createUuidArray(connection, entityKeyIds))
            val rs = ps.executeQuery()

            while (rs.next()) {
                entries.put(ResultSetAdapters.id(rs), ResultSetAdapters.entityKey(rs))
            }
        }

        return entries
    }

}