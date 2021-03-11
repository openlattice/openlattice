package com.openlattice.data.jobs

import com.geekbeast.rhizome.jobs.AbstractDistributedJob
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.openlattice.data.DeleteType
import com.openlattice.data.EntityDataKey
import com.openlattice.data.storage.PostgresEntitySetSizesInitializationTask
import com.openlattice.data.storage.partitions.getPartition
import com.openlattice.data.storage.updateVersionsForEntitiesInEntitySet
import com.openlattice.data.storage.zeroVersionsForEntitiesInEntitySet
import com.openlattice.edm.EntitySet
import com.openlattice.edm.processors.GetPartitionsFromEntitySetEntryProcessor
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.serializers.decorators.MetastoreAware
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.COUNT
import com.openlattice.postgres.PostgresColumn.DST_ENTITY_KEY_ID
import com.openlattice.postgres.PostgresColumn.DST_ENTITY_SET_ID
import com.openlattice.postgres.PostgresColumn.EDGE_ENTITY_KEY_ID
import com.openlattice.postgres.PostgresColumn.EDGE_ENTITY_SET_ID
import com.openlattice.postgres.PostgresColumn.ENTITY_SET_ID
import com.openlattice.postgres.PostgresColumn.ID
import com.openlattice.postgres.PostgresColumn.PARTITION
import com.openlattice.postgres.PostgresColumn.SRC_ENTITY_KEY_ID
import com.openlattice.postgres.PostgresColumn.SRC_ENTITY_SET_ID
import com.openlattice.postgres.PostgresColumn.VERSION
import com.openlattice.postgres.PostgresColumn.VERSIONS
import com.openlattice.postgres.PostgresTable.DATA
import com.openlattice.postgres.PostgresTable.E
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.util.*

class DataDeletionJob(
        state: DataDeletionJobState
) : AbstractDistributedJob<Long, DataDeletionJobState>(state), MetastoreAware {

    companion object {
        private const val BATCH_SIZE = 10_000
        private val ALL_PARTITIONS = (0..257)
    }

    @Transient
    private lateinit var hds: HikariDataSource

    @Transient
    private lateinit var entitySets: IMap<UUID, EntitySet>

    override fun initialize() {
        state.totalToDelete = getTotalToDelete()
    }

    override fun processNextBatch() {
        val entityDataKeys = getBatchOfEntityDataKeys()

        if (entityDataKeys.isEmpty()) {
            hasWorkRemaining = false
            publishJobState()
            return
        }

        var edgeBatch = getBatchOfEdgesForIds(entityDataKeys)
        while (edgeBatch.isNotEmpty()) {
            deleteEntities(edgeBatch)
            deleteEdges(edgeBatch)
            edgeBatch = getBatchOfEdgesForIds(entityDataKeys)
        }

        state.numDeletes += deleteEntities(entityDataKeys)
        publishJobState()
    }

    override fun updateProgress() {
        progress = ((100 * state.numDeletes) / state.totalToDelete).toByte()
    }

    private fun getTotalToDelete(): Long {
        state.entityKeyIds?.let {
            return it.size.toLong()
        }

        return hds.connection.use { connection ->
            connection.prepareStatement(GET_ENTITY_SET_COUNT_SQL).use { ps ->
                ps.setObject(1, state.entitySetId)
                ps.executeQuery().use { rs ->
                    if (rs.next()) {
                        rs.getLong(1)
                    } else {
                        0
                    }
                }
            }
        }
    }

    private fun getBatchOfEntityDataKeys(): Set<EntityDataKey> {
        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, getIdsBatchSql()) {
            it.setObject(1, state.entitySetId)
            it.setArray(2, PostgresArrays.createIntArray(it.connection, state.partitions))
        }) {
            ResultSetAdapters.entityDataKey(it)
        }.toSet()
    }

    private fun getBatchOfEdgesForIds(edks: Set<EntityDataKey>): Set<EntityDataKey> {
        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, getEdgesBatchSql()) {
            val entityKeyIdsArr = PostgresArrays.createUuidArray(it.connection, edks.map { edk -> edk.entityKeyId })
            it.setObject(1, state.entitySetId)
            it.setArray(2, entityKeyIdsArr)
            it.setObject(3, state.entitySetId)
            it.setArray(4, entityKeyIdsArr)
            it.setObject(5, state.entitySetId)
            it.setArray(6, entityKeyIdsArr)
        }) {
            EntityDataKey(ResultSetAdapters.edgeEntitySetId(it), ResultSetAdapters.edgeEntityKeyId(it))
        }.toSet()
    }

    private fun deleteEntities(entityDataKeys: Set<EntityDataKey>): Int {
        val entitySetIdToPartitions = getEntitySetPartitions(entityDataKeys)

        val (deleteFromDataSql, deleteFromIdsSql) = if (state.deleteType == DeleteType.Hard) {
            HARD_DELETE_FROM_DATA_SQL to HARD_DELETE_FROM_IDS_SQL
        } else {
            SOFT_DELETE_FROM_DATA_SQL to SOFT_DELETE_FROM_IDS_SQL
        }

        val entitySetIdToPartitionToIds = entityDataKeys.groupBy { it.entitySetId }.mapValues { (entitySetId, edks) ->
            val partitions = entitySetIdToPartitions.getValue(entitySetId).toList()
            edks.map { it.entityKeyId }.groupBy { getPartition(it, partitions) }
        }
        val version = -System.currentTimeMillis()

        return hds.connection.use { conn ->
            conn.prepareStatement(deleteFromDataSql).use { ps ->
                entitySetIdToPartitionToIds.forEach { (entitySetId, partitionToIds) ->
                    partitionToIds.forEach { (partition, ids) ->
                        bindEntityDelete(ps, entitySetId, partition, ids, version)
                    }
                }
                ps.executeBatch()
            }

            conn.prepareStatement(deleteFromIdsSql).use { ps ->
                entitySetIdToPartitionToIds.forEach { (entitySetId, partitionToIds) ->
                    partitionToIds.forEach { (partition, ids) ->
                        bindEntityDelete(ps, entitySetId, partition, ids, version)
                    }
                }
                ps.executeBatch()
            }.sum()
        }
    }

    private fun getEntitySetPartitions(entityDataKeys: Set<EntityDataKey>): Map<UUID, Iterable<Int>> {
        val entitySetIds = entityDataKeys.mapTo(mutableSetOf()) { it.entitySetId }
        val esToPartitions = entitySets.executeOnKeys(entitySetIds, GetPartitionsFromEntitySetEntryProcessor())

        return entitySetIds.associateWith {
            esToPartitions[it] ?: if (it == state.entitySetId) {
                state.partitions
            } else {
                ALL_PARTITIONS
            }
        }
    }

    private fun bindEntityDelete(ps: PreparedStatement, entitySetId: UUID, partition: Int, ids: Collection<UUID>, version: Long) {
        var index = 1
        if (state.deleteType == DeleteType.Soft) {
            ps.setLong(index++, version)
            ps.setLong(index++, version)
            ps.setLong(index++, version)
        }
        ps.setObject(index++, entitySetId)
        ps.setInt(index++, partition)
        ps.setArray(index, PostgresArrays.createUuidArray(ps.connection, ids))
    }

    private fun deleteEdges(edgeBatch: Set<EntityDataKey>) {
        val sql = if (state.deleteType == DeleteType.Hard) hardDeleteEdgesSql() else softDeleteEdgesSql()
        val version = -System.currentTimeMillis()

        hds.connection.use { conn ->
            conn.prepareStatement(sql).use { ps ->
                edgeBatch.forEach { bindEdgeDelete(ps, it, version) }
                ps.executeBatch()
            }
        }
    }

    private fun bindEdgeDelete(ps: PreparedStatement, edk: EntityDataKey, version: Long) {
        var index = 1
        if (state.deleteType == DeleteType.Soft) {
            ps.setLong(index++, version)
            ps.setLong(index++, version)
        }
        ps.setObject(index++, edk.entitySetId)
        ps.setObject(index, edk.entityKeyId)
    }

    override fun setHikariDataSource(hds: HikariDataSource) {
        this.hds = hds
    }

    override fun setHazelcastInstance(hazelcastInstance: HazelcastInstance) {
        super.setHazelcastInstance(hazelcastInstance)
        this.entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)
    }

    private fun excludeClearedIfSoftDeleteSql(): String {
        return if (state.deleteType == DeleteType.Hard) {
            ""
        } else {
            "AND ${VERSION.name} > 0"
        }
    }

    /**
     * PreparedStatement bind order:
     *
     * 1) entitySetId
     */
    private val GET_ENTITY_SET_COUNT_SQL = """
        SELECT $COUNT 
            FROM ${PostgresEntitySetSizesInitializationTask.ENTITY_SET_SIZES_VIEW} 
            WHERE ${ENTITY_SET_ID.name} = ?
    """.trimIndent()

    /**
     * PreparedStatement bind order:
     *
     * 1) entitySetId
     * 2) partitions
     */
    private fun getIdsBatchSql(): String {
        return """
            SELECT ${ID.name}
            FROM ${IDS.name}
            WHERE ${ENTITY_SET_ID.name} = ?
            AND ${PARTITION.name} = ANY(?)
            ${excludeClearedIfSoftDeleteSql()}
            LIMIT $BATCH_SIZE
        """.trimIndent()
    }

    /**
     * PreparedStatement bind order:
     *
     * 1) entitySetId
     * 2) entityKeyIds
     * 3) entitySetId
     * 4) entityKeyIds
     * 5) entitySetId
     * 6) entityKeyIds
     */
    private fun getEdgesBatchSql(): String {
        val entityMatches = listOf(
                SRC_ENTITY_SET_ID to SRC_ENTITY_KEY_ID,
                EDGE_ENTITY_SET_ID to EDGE_ENTITY_KEY_ID,
                DST_ENTITY_SET_ID to DST_ENTITY_KEY_ID
        ).joinToString(" OR ") { (entitySetIdCol, entityKeyIdCol) ->
            "( ${entitySetIdCol.name} = ? AND ${entityKeyIdCol.name} = ANY(?) )"
        }

        return """
            SELECT ${EDGE_ENTITY_SET_ID.name}, ${EDGE_ENTITY_KEY_ID.name}
            FROM ${E.name}
            WHERE ( $entityMatches )
            ${excludeClearedIfSoftDeleteSql()}
            LIMIT $BATCH_SIZE
        """.trimIndent()
    }

    /**
     * PreparedStatement bind order:
     *
     * 1. version
     * 2. version
     * 3. version
     * 4. entitySetId
     * 5. partition
     * 6. entityKeyIds
     */
    private val SOFT_DELETE_FROM_DATA_SQL = """
        UPDATE ${DATA.name} 
        SET
          ${VERSIONS.name} = ${VERSIONS.name} || ARRAY[?], 
          ${VERSION.name} = 
            CASE
              WHEN abs(${DATA.name}.${VERSION.name}) <= abs(?)
              THEN ?
              ELSE ${DATA.name}.${VERSION.name} 
            END,
          ${LAST_WRITE.name} = 'now()'
        WHERE
          ${ENTITY_SET_ID.name} = ?
          AND ${PARTITION.name} = ?
          AND ${ID.name} = ANY(?)
            
    """.trimIndent()

    /**
     * PreparedStatement bind order:
     *
     * 1. entitySetId
     * 2. partition
     * 3. entityKeyIds
     */
    private val HARD_DELETE_FROM_DATA_SQL = """
        DELETE FROM ${DATA.name}
        WHERE
          ${ENTITY_SET_ID.name} = ?
          AND ${PARTITION.name} = ?
          AND ${ID.name} = ANY(?)
    """.trimIndent()

    /**
     * PreparedStatement bind order:
     *
     * 1. entitySetId
     * 2. partition
     * 3. entityKeyIds
     */
    private val HARD_DELETE_FROM_IDS_SQL = zeroVersionsForEntitiesInEntitySet

    /**
     * 1. version
     * 2. version
     * 3. version
     * 4. entitySetId
     * 5. partition
     * 6. entityKeyIds
     */
    private val SOFT_DELETE_FROM_IDS_SQL = updateVersionsForEntitiesInEntitySet
    /**
     * PreparedStatement bind order:
     *
     * 1) version
     * 2) version
     * 1) entitySetIds
     * 2) entityKeyIds
     */
    private fun softDeleteEdgesSql(): String {
        return """
            UPDATE ${E.name}
            SET
              ${VERSION.name} = ?,
              ${VERSIONS.name} = ${VERSIONS.name} || ?,
              ${LAST_WRITE.name} = now()
            WHERE
              ${EDGE_ENTITY_SET_ID.name} = ANY(?)
              AND ${EDGE_ENTITY_KEY_ID.name} = ANY(?) 
            
        """.trimIndent()
    }

    /**
     * PreparedStatement bind order:
     *
     * 1) entitySetIds
     * 2) entityKeyIds
     */
    private fun hardDeleteEdgesSql(): String {
        return """
            DELETE FROM ${E.name}
            WHERE
              ${EDGE_ENTITY_SET_ID.name} = ANY(?)
              AND ${EDGE_ENTITY_KEY_ID.name} = ANY(?) 
        """.trimIndent()
    }
}