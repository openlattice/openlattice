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

import com.geekbeast.configuration.postgres.PostgresFlavor
import com.openlattice.IdConstants
import com.openlattice.data.EntityDataKey
import com.openlattice.data.EntityKey
import com.openlattice.data.EntityKeyIdService
import com.openlattice.data.storage.DataSourceResolver
import com.openlattice.data.storage.getByDataSource
import com.openlattice.data.util.PostgresDataHasher
import com.openlattice.ids.HazelcastIdGenerationService
import com.openlattice.postgres.*
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.*
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.sql.Connection
import java.sql.PreparedStatement
import java.util.*
import kotlin.collections.HashMap

internal val entityKeyIdsSqlNotIdWritten =
    "SELECT * FROM ${SYNC_IDS.name} WHERE ${ENTITY_SET_ID.name} = ? AND ${ENTITY_ID.name} = ANY(?) AND NOT ${ID_WRITTEN.name} "
private val entityKeyIdsSqlAny =
    "SELECT * FROM ${SYNC_IDS.name} WHERE ${ENTITY_SET_ID.name} = ? AND ${ENTITY_ID.name} = ANY(?)"
private val entityKeyIdSql = "SELECT * FROM ${SYNC_IDS.name} WHERE ${ENTITY_SET_ID.name} = ? AND ${ENTITY_ID.name} = ? "
private val linkedEntityKeyIdsSql =
    "SELECT ${ID.name},${LINKING_ID.name} as $ENTITY_KEY_IDS_FIELD FROM ${IDS.name} WHERE ${ID.name} = ANY(?) AND ${LINKING_ID.name} IS NOT NULL"

//Only update ids the need updating to minimize i/o
private val UPDATE_ID_WRITTEN =
    "UPDATE ${SYNC_IDS.name} SET ${ID_WRITTEN.name} = TRUE WHERE ${ENTITY_SET_ID.name} = ? AND ${ENTITY_ID.name} = ? AND NOT ${ID_WRITTEN.name}"
private val INSERT_SQL = "INSERT INTO ${IDS.name} (${ENTITY_SET_ID.name},${ID.name}) VALUES(?,?) ON CONFLICT DO NOTHING"
private val INSERT_ID_TO_DATA_SQL = "INSERT INTO ${DATA.name} (" +
        "${ENTITY_SET_ID.name}," +
        "${ID.name}," +
        "${PROPERTY_TYPE_ID.name}," +
        "${VERSION.name}," +
        "${HASH.name}) VALUES (?,?,?,?,?) " +
        "ON CONFLICT DO NOTHING"
internal val INSERT_SYNC_SQL =
    "INSERT INTO ${SYNC_IDS.name} (${ENTITY_SET_ID.name},${ENTITY_ID.name},${ID.name}) VALUES(?,?,?) ON CONFLICT DO NOTHING"

private val logger = LoggerFactory.getLogger(PostgresEntityKeyIdService::class.java)

/**
 * This service is responsible for assigning ids to entity keys and persisting the mapping in postgres.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Service
class PostgresEntityKeyIdService(
    private val dataSourceResolver: DataSourceResolver,
    private val idGenerationService: HazelcastIdGenerationService
) : EntityKeyIdService {

    private fun genEntityKeyIds(entityIds: Set<EntityKey>): Map<EntityKey, UUID> {
        val ids = idGenerationService.getNextIds(entityIds.size)
        require(ids.size == entityIds.size) { "Insufficient ids generated." }
        return entityIds.zip(ids).toMap()
    }

    private fun storeEntityKeyIdReservations(
        entitySetId: UUID,
        entityKeyIds: Set<UUID>,
    ) {
        val hds = dataSourceResolver.resolve(entitySetId)

        hds.connection.use { connection ->
            val insertIds = connection.prepareStatement(INSERT_SQL)
            val insertToData = connection.prepareStatement(INSERT_ID_TO_DATA_SQL)

            entityKeyIds.forEach { entityKeyId ->
                storeEntityKeyIdAddBatch(entitySetId, entityKeyId, insertIds, insertToData)
            }

            val totalWritten = insertIds.executeBatch().sum()
            val totalDataRowsWritten = insertToData.executeBatch().sum()
            logger.debug("Inserted $totalWritten ids.")
            logger.debug("Inserted $totalDataRowsWritten data ids.")
        }

    }


    private fun getEntityKeyIdsByDataSource(entityKeyIds: Map<EntityKey, UUID>): Map<String, Map<EntityKey, UUID>> {
        return getByDataSource(dataSourceResolver, entityKeyIds) { it.entitySetId }
    }

    private fun storeEntityKeyIds(entityKeyIds: Map<EntityKey, UUID>): Map<EntityKey, UUID> {
        val entityKeyIdsByDatasource = getEntityKeyIdsByDataSource(entityKeyIds)

        return entityKeyIdsByDatasource.map { (datasourceName, entityKeyIdGroup) ->
            val flavor = dataSourceResolver.getFlavor(datasourceName)
            dataSourceResolver.getDataSource(datasourceName).connection.use { connection ->
                storeEntityKeyIds(connection, entityKeyIdGroup, idGenerationService, flavor)
            }
        }.flatMap { entityKeyIdGroup -> entityKeyIdGroup.entries.map { it.toPair() } }.toMap()
    }

    private fun assignEntityKeyIds(entityKeys: Set<EntityKey>): Map<EntityKey, UUID> {
        val assignedEntityKeyIds = entityKeys.associateWith { idGenerationService.getNextId() }
        return storeEntityKeyIds(assignedEntityKeyIds)
    }

    override fun reserveEntityKeyIds(entityKeys: Set<EntityKey>): Set<UUID> {
        val entityIdsByEntitySet = entityKeys
            .groupBy({ it.entitySetId }, { it.entityId })
            .mapValues { it.value.toSet() }
        val existing = loadEntityKeyIds(entityIdsByEntitySet)
        val missing = entityKeys - existing.keys
        val missingMap = assignEntityKeyIds(missing)

        return entityKeys.asSequence().map { existing[it] ?: missingMap.getValue(it) }.toSet()
    }

    override fun reserveLinkingIds(count: Int): List<UUID> {
        val ids = idGenerationService.getNextIds(count)
        storeEntityKeyIdReservations(
            IdConstants.LINKING_ENTITY_SET_ID.id,
            ids
        )
        return ids.toList()
    }

    override fun reserveIds(entitySetId: UUID, count: Int): List<UUID> {
        val ids = idGenerationService.getNextIds(count)
        storeEntityKeyIdReservations(
            entitySetId,
            ids
        )
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
        return dataSourceResolver.resolve(entitySetId).connection.use {
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

    private fun loadEntityKeyIds(
        entityIds: Map<UUID, Set<String>>,
        allIdWritten: Boolean = false
    ): MutableMap<EntityKey, UUID> {
        return getByDataSource(
            dataSourceResolver,
            entityIds
        ) { it }.asSequence().flatMap { (datasourceName, entityIdGroup) ->
            dataSourceResolver.getDataSource(datasourceName).connection.use { connection ->
                loadEntityKeyIds(connection, entityIdGroup, allIdWritten)
            }.entries
        }.map { it.toPair() }.toMap(mutableMapOf())
    }

    /**
     *
     * @param ids A map of entity set ids to entity key ids.
     * @return A map of entity key ids to linking ids
     */
    override fun getLinkingEntityKeyIds(ids: Map<UUID, Set<UUID>>): Map<EntityDataKey, UUID> {
        return ids.asSequence().flatMap { (entitySetId, entityKeyIds) ->
            val hds = dataSourceResolver.resolve(entitySetId)
            BasePostgresIterable(PreparedStatementHolderSupplier(hds, linkedEntityKeyIdsSql) {
                it.setArray(1, PostgresArrays.createUuidArray(it.connection, entityKeyIds))
            }) {
                EntityDataKey(
                    it.getObject(ENTITY_SET_ID.name, UUID::class.java),
                    it.getObject(ID.name, UUID::class.java)
                ) to it.getObject(LINKING_ID.name, UUID::class.java)
            }
        }.toMap()
    }

    override fun getEntityKeyIds(
        entityKeys: Set<EntityKey>,
        entityKeyIds: MutableMap<EntityKey, UUID>
    ): MutableMap<EntityKey, UUID> {
        val entityIdsByEntitySet = entityKeys
            .groupBy({ it.entitySetId }, { it.entityId })
            .mapValues { it.value.toSet() }
        entityKeyIds.putAll(loadEntityKeyIds(entityIdsByEntitySet))

        //Making this line O(n) is why we chose to just take a set instead of a sequence (thus allowing lazy views since copy is required anyway)
        val missing = entityKeys.minus(entityKeyIds.keys)

        val missingMap = assignEntityKeyIds(missing)
        entityKeyIds.putAll(missingMap)

        return entityKeyIds
    }

    override fun getEntityKeyIds(entityKeys: Set<EntityKey>): MutableMap<EntityKey, UUID> {
        return getEntityKeyIds(entityKeys, HashMap(entityKeys.size))
    }

}

internal fun createRedshiftMergeTableSql(index: Long): String {
    return PostgresTableDefinition(syncIds(index))
        .addColumns(*SYNC_IDS.columns.toTypedArray())
        .primaryKey(*SYNC_IDS.primaryKey.toTypedArray())
        .temporary()
        .createTableQuery()
}

internal fun getRedshiftInsertSyncSql(index: Long): String {
    return "INSERT INTO ${syncIds(index)} (${ENTITY_SET_ID.name},${ENTITY_ID.name},${ID.name}) VALUES(?,?,?)"
}

internal fun deleteExistingSyncSql(index: Long): String {
    return """
            DELETE FROM ${syncIds(index)} USING ${SYNC_IDS.name} 
            WHERE ${syncIds(index)}.${ENTITY_SET_ID.name} = ${SYNC_IDS.name}.${ENTITY_SET_ID.name} AND
            ${syncIds(index)}.${ENTITY_ID.name} = ${SYNC_IDS.name}.${ENTITY_ID.name}
        """.trimIndent()
}

internal fun insertFromTempTableSql(index: Long): String {
    return """
            INSERT INTO ${SYNC_IDS.name} SELECT * FROM ${syncIds(index)} 
        """.trimIndent()
}

internal fun syncIds(index: Long): String {
    return "sync_ids_$index"
}

internal fun loadEntityKeyIds(
    connection: Connection,
    entityIds: Map<UUID, Set<String>>,
    allIdWritten: Boolean = false
): MutableMap<EntityKey, UUID> {
    val ids = HashMap<EntityKey, UUID>(entityIds.values.sumBy { it.size })

    val ps = connection.prepareStatement(if (allIdWritten) entityKeyIdsSqlAny else entityKeyIdsSqlNotIdWritten)
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

internal fun storeEntityKeyIds(
    connection: Connection,
    entityKeyIds: Map<EntityKey, UUID>,
    idGenerationService: HazelcastIdGenerationService,
    flavor: PostgresFlavor
): Map<EntityKey, UUID> {
    /**
     * The new algorithm will result in more reads, but reduce the amount of hazelcast traffic
     * for locking during id generation. This will only happen if a request to assign entity key ids
     * to a large number of overlapping sets of entity keys not already in the system. Otherwise,
     * this function won't be called as the assignment will be read further up.
     *
     * 1. Attempt to insert into sync ids (will silently fail if already inserted.
     * 2. Load all actual entity key ids for sync ids
     * 3. Issue batched inserts for actual entity key ids. May end up performin a lot of no-ops.
     */

    /**
     * We don't need to commit this as a single transaction as it is fail-safe. If client fails in the middle
     * of a request, retrying the request with identical entity key will successfully complete the assignment process.
     */


    val insertSyncIds = connection.prepareStatement(INSERT_SYNC_SQL)

    entityKeyIds.forEach {
        insertSyncIds.setObject(1, it.key.entitySetId)
        insertSyncIds.setString(2, it.key.entityId)
        insertSyncIds.setObject(3, it.value)
        insertSyncIds.addBatch()
    }

    val totalSyncIdRowsWritten = insertSyncIds.executeBatch().sum()

    val syncIds = entityKeyIds.keys
        .groupBy({ it.entitySetId }, { it.entityId })
        .mapValues { it.value.toSet() }

    /**
     * Load all entity key ids even if assignment has been finalized.
     */

    val actualEntityKeyIds = loadEntityKeyIds(connection, syncIds, true)

    val insertIds = connection.prepareStatement(INSERT_SQL)
    val insertToData = connection.prepareStatement(INSERT_ID_TO_DATA_SQL)
    val updateIdsWritten = connection.prepareStatement(UPDATE_ID_WRITTEN)

    actualEntityKeyIds.forEach {
        storeEntityKeyIdAddBatch(
            it.key.entitySetId,
            entityKeyId = it.value,
            insertIds = insertIds,
            insertToData = insertToData,
        )
    }

    val totalWritten = insertIds.executeBatch().sum()
    val totalDataRowsWritten = insertToData.executeBatch().sum()

    //Mark all the ids as updated / finalized.
    syncIds.forEach { (entitySetId, entityIds) ->
        entityIds.forEach { entityId ->
            updateIdsWritten.setObject(1, entitySetId)
            updateIdsWritten.setObject(2, entityId)
            updateIdsWritten.addBatch()
        }
    }

    val idsWrittenCount = updateIdsWritten.executeBatch().sum()

    if (logger.isDebugEnabled) {
        logger.debug("Inserted $totalSyncIdRowsWritten sync ids.")
        logger.debug("Inserted $totalWritten ids.")
        logger.debug("Inserted $totalDataRowsWritten data ids.")
        logger.debug("Updated $idsWrittenCount id written flags.")
    }


    //Take the actual entity key ids of instead of the generated ones.
    return entityKeyIds.mapValues {
        //Actual entity key ids should have all entity key ids
        val actualEntityKeyId = actualEntityKeyIds.getValue(it.key)
        return@mapValues if (actualEntityKeyId != it.value) {
            idGenerationService.returnId(it.value)
            actualEntityKeyId
        } else {
            it.value
        }
    }

}

internal fun storeEntityKeyIdAddBatch(
    entitySetId: UUID,
    entityKeyId: UUID,
    insertIds: PreparedStatement,
    insertToData: PreparedStatement,
) {

    insertIds.setObject(1, entitySetId)
    insertIds.setObject(2, entityKeyId)
    insertIds.addBatch()

    insertToData.setObject(1, entitySetId)
    insertToData.setObject(2, entityKeyId)
    insertToData.setObject(3, IdConstants.ID_ID.id)
    insertToData.setLong(4, System.currentTimeMillis())
    insertToData.setObject(5, PostgresDataHasher.hashObject(entityKeyId, EdmPrimitiveTypeKind.Guid))
    insertToData.addBatch()
}
