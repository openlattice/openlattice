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
import com.google.common.collect.Queues
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.openlattice.data.EntityKey
import com.openlattice.data.EntityKeyIdService
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.ids.HazelcastIdGenerationService
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.PostgresTable.SYNC_IDS
import com.openlattice.postgres.ResultSetAdapters
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.BlockingQueue
import kotlin.collections.HashMap

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
private val entityKeysSql = "SELECT * FROM ${IDS.name} WHERE ${ID.name} = ANY(?) "
private val entityKeyIdsSql = "SELECT * FROM ${SYNC_IDS.name} WHERE ${ENTITY_SET_ID.name} = ? AND ${ENTITY_ID.name} = ANY(?) "
private val entityKeyIdSql = "SELECT * FROM ${SYNC_IDS.name} WHERE ${ENTITY_SET_ID.name} = ? AND ${ENTITY_ID.name} = ? "
private val INSERT_SQL = "INSERT INTO ${IDS.name} (${ENTITY_SET_ID.name},${ID.name}) VALUES(?,?)"
private val INSERT_SYNC_SQL = "INSERT INTO ${SYNC_IDS.name} (${ENTITY_SET_ID.name},${ENTITY_ID.name},${ID.name}) VALUES(?,?,?)"

private val logger = LoggerFactory.getLogger(PostgresEntityKeyIdService::class.java)

class PostgresEntityKeyIdService(
        hazelcastInstance: HazelcastInstance,
        private val executor: ListeningExecutorService,
        private val hds: HikariDataSource,
        private val idGenerationService: HazelcastIdGenerationService
) : EntityKeyIdService {
    private val q: BlockingQueue<UUID> = Queues.newArrayBlockingQueue(65536)
    private val idRefCounts = hazelcastInstance.getMap<EntityKey, Long>(HazelcastMap.ID_REF_COUNTS.name)
    private val idMap = hazelcastInstance.getMap<EntityKey, UUID>(HazelcastMap.ID_CACHE.name)

    init {
        idGenerationService.getNextIds(q.remainingCapacity()).forEach(q::put)
    }

    private fun genEntityKeyIds(entityIds: Set<EntityKey>): Map<EntityKey, UUID> {
        val ids = idGenerationService.getNextIds(entityIds.size)
        checkState(ids.size == entityIds.size, "Insufficient ids generated.")
        return entityIds.zip(ids).toMap()
    }

    private fun storeEntityKeyIdReservations(entitySetId: UUID, entityKeyIds: Set<UUID>) {
        hds.connection.use { connection ->

            val insertIds = connection.prepareStatement(INSERT_SQL)

            entityKeyIds.forEach { entityKeyId ->
                insertIds.setObject(1, entitySetId)
                insertIds.setObject(3, entityKeyId)
                insertIds.addBatch()
            }

            val totalWritten = insertIds.executeBatch().sum()
            if (totalWritten != entityKeyIds.size) {
                logger.warn("Expected ${entityKeyIds.size} entity key id writes. Only $totalWritten writes registered.")
            }
        }
    }

    private fun storeEntityKeyIds(entityKeyIds: Map<EntityKey, UUID>): Map<EntityKey, UUID> {
        hds.connection.use { connection ->
            connection.autoCommit = false

            val insertSyncIds = connection.prepareStatement(INSERT_SYNC_SQL)

            entityKeyIds.forEach {
                insertSyncIds.setObject(1, it.key.entitySetId)
                insertSyncIds.setString(2, it.key.entityId)
                insertSyncIds.setObject(3, it.value)
                insertSyncIds.addBatch()
            }

            val insertIds = connection.prepareStatement(INSERT_SQL)

            entityKeyIds.forEach {
                insertIds.setObject(1, it.key.entitySetId)
                insertIds.setObject(2, it.value)
                insertIds.addBatch()
            }

            val totalWritten = insertIds.executeBatch().sum()
            if (totalWritten != entityKeyIds.size) {
                logger.warn("Expected ${entityKeyIds.size} entity key writes. Only $totalWritten writes registered.")
            }
            connection.commit()
            connection.autoCommit = true
        }
        return entityKeyIds
    }

    private fun countUpEntityKeys(entityKeys: Set<EntityKey>) {
        idRefCounts.executeOnKeys(entityKeys, IdRefCountIncrementer())

    }

    private fun countDownEntityKeys(entityKeys: Set<EntityKey>) {
        idRefCounts.executeOnKeys(entityKeys, IdRefCountDecrementer())
                .forEach { entityKey, count ->
                    if (count == 0) {
                        idMap.delete(entityKey)
                    }
                }
    }

    private fun assignEntityKeyIds(entityKeys: Set<EntityKey>): Map<EntityKey, UUID> {
        executor.submit {
            idGenerationService.getNextIds(entityKeys.size).forEach(q::put)
        }

        return entityKeys.map { key ->
            val elem = q.take()
            val assignedId = idMap.putIfAbsent(key, elem)
            return@map if (assignedId == null) {
                val assignedPair = key to elem
                storeEntityKeyIds(mapOf(assignedPair))
                assignedPair
            } else {
                q.offer(elem)
                key to assignedId
            }
        }.toMap()
    }

    override fun reserveEntityKeyIds(entityKeys: Set<EntityKey>): Set<UUID> {
        val entityIdsByEntitySet = entityKeys.groupBy({ it.entitySetId },
                                                      { it.entityId }).mapValues { it.value.toSet() }
        val existing = loadEntityKeyIds(entityIdsByEntitySet)
        val missing = entityKeys - existing.keys

        countUpEntityKeys(missing)

        val missingMap = assignEntityKeyIds(missing)

        countDownEntityKeys(missing)

        return entityKeys.asSequence().map { existing[it] ?: missingMap.getValue(it) }.toSet()

    }

    override fun reserveIds(entitySetId: UUID, count: Int): List<UUID> {
        val ids = idGenerationService.getNextIds(count)
        storeEntityKeyIdReservations(entitySetId, ids)
        return ids.toList()
    }

    override fun getEntityKeyId(entitySetId: UUID, entityId: String): UUID {
        val entityKey = EntityKey(entitySetId, entityId)
        return getEntityKeyId(entityKey)

    }

    override fun getEntityKeyId(entityKey: EntityKey): UUID {
        return loadEntityKeyId(entityKey.entitySetId, entityKey.entityId) ?: storeEntityKeyIds(
                genEntityKeyIds(setOf(entityKey))
        ).getValue(entityKey)
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

    private fun loadEntityKeyIds(entityIds: Map<UUID, Set<String>>): MutableMap<EntityKey, UUID> {
        return hds.connection.use { connection ->

            val ids = HashMap<EntityKey, UUID>(entityIds.values.sumBy { it.size })

            val ps = connection.prepareStatement(entityKeyIdsSql)
            entityIds
                    .forEach { (entitySetId, entityIdValues) ->
                        ps.setObject(1, entitySetId)
                        ps.setArray(2, PostgresArrays.createTextArray(connection, entityIdValues))
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
        val entityIdsByEntitySet = entityKeys.groupBy({ it.entitySetId },
                                                      { it.entityId }).mapValues { it.value.toSet() }
        entityKeyIds.putAll(loadEntityKeyIds(entityIdsByEntitySet))

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