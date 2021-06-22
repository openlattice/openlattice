package com.openlattice.data.jobs

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.geekbeast.rhizome.jobs.AbstractDistributedJob
import com.geekbeast.rhizome.jobs.JobStatus
import com.google.common.collect.Sets
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.openlattice.data.*
import com.openlattice.data.storage.*
import com.openlattice.data.storage.partitions.getPartition
import com.openlattice.edm.EntitySet
import com.openlattice.edm.processors.GetEntityTypeFromEntitySetEntryProcessor
import com.openlattice.edm.processors.GetPartitionsFromEntitySetEntryProcessor
import com.openlattice.edm.processors.GetPropertiesFromEntityTypeEntryProcessor
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.edge.Edge
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.serializers.decorators.ByteBlobDataManagerAware
import com.openlattice.hazelcast.serializers.decorators.DataGraphAware
import com.openlattice.hazelcast.serializers.decorators.MetastoreAware
import com.openlattice.ioc.providers.LateInitAware
import com.openlattice.ioc.providers.LateInitProvider
import com.openlattice.linking.graph.PostgresLinkingQueryService
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
import com.openlattice.postgres.PostgresDatatype
import com.openlattice.postgres.PostgresTable.DATA
import com.openlattice.postgres.PostgresTable.E
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import java.sql.PreparedStatement
import java.util.*

class DataDeletionJob(
    state: DataDeletionJobState
) : AbstractDistributedJob<Long, DataDeletionJobState>(state),
    LateInitAware {

    @JsonCreator
    constructor(
        id: UUID?,
        taskId: Long?,
        status: JobStatus,
        progress: Byte,
        hasWorkRemaining: Boolean,
        result: Long?,
        state: DataDeletionJobState
    ) : this(state) {
        initialize(id, taskId, status, progress, hasWorkRemaining, result)
    }

    override val resumable = true

    companion object {
        private const val BATCH_SIZE = 10_000
        private val ALL_PARTITIONS = (0..257)
    }

    @Transient
    private lateinit var lateInitProvider: LateInitProvider

    @Transient
    private lateinit var entitySets: IMap<UUID, EntitySet>

    @Transient
    private lateinit var entityTypes: IMap<UUID, EntityType>

    @Transient
    private lateinit var propertyTypes: IMap<UUID, PropertyType>

    override fun initializeHazelcastRelatedObjects() {
        this.entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)
        this.entityTypes = HazelcastMap.ENTITY_TYPES.getMap(hazelcastInstance)
        this.propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap(hazelcastInstance)
    }

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
            logger.info("Deleting edges and entities involving {}", edgeBatch)
            val edgeEdkBatch = edgeBatch.map { it.edge }.toSet()
            deleteEntities(edgeEdkBatch)
            deleteEdges(edgeBatch)
            edgeBatch = getBatchOfEdgesForIds(entityDataKeys)
        }

        state.numDeletes += deleteEntities(entityDataKeys)
        cleanUpBatch(entityDataKeys)
        publishJobState()
    }

    override fun updateProgress() {
        if (state.totalToDelete > 0) {
            progress = ((100 * state.numDeletes) / state.totalToDelete).toByte()
        }
    }

    @JsonIgnore
    private fun getTotalToDelete(): Long {
        state.entityKeyIds?.let {
            return it.size.toLong()
        }
        val hds = lateInitProvider.resolver.getDefaultDataSource()
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

    @JsonIgnore
    private fun getBatchOfEntityDataKeys(): Set<EntityDataKey> {
        state.entityKeyIds?.let { entityKeyIds ->
            return entityKeyIds.take(BATCH_SIZE).mapTo(mutableSetOf()) { EntityDataKey(state.entitySetId, it) }
        }

        val hds = lateInitProvider.resolver.getDefaultDataSource()

        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, getIdsBatchSql()) {
            it.setObject(1, state.entitySetId)
            it.setArray(2, PostgresArrays.createIntArray(it.connection, state.partitions))
        }) {
            ResultSetAdapters.entityDataKey(it)
        }.toSet()
    }

    private fun cleanUpBatch(entityDataKeys: Set<EntityDataKey>) {
        val entityKeyIds = entityDataKeys.mapTo(mutableSetOf()) { it.entityKeyId }
        val hds = lateInitProvider.resolver.getDefaultDataSource()
        PostgresLinkingQueryService.deleteNeighborhoods(hds, state.entitySetId, entityKeyIds)
        state.entityKeyIds?.removeAll(entityKeyIds)
    }

    @JsonIgnore
    /**
     * @param edks All these entity data keys should be from the same entity set
     */
    private fun getBatchOfEdgesForIds(edks: Set<EntityDataKey>): Set<DataEdgeKey> {
        return edks
            .groupBy { lateInitProvider.resolver.getDataSourceName(it.entitySetId) }
            .flatMap { (dataSourceName, entityDataKeysForDataSource) ->
                val hds = lateInitProvider.resolver.getDataSource(dataSourceName)
                BasePostgresIterable(PreparedStatementHolderSupplier(hds, getEdgesBatchSql()) {
                    val entityKeyIdsArr = PostgresArrays.createUuidArray(
                        it.connection,
                        entityDataKeysForDataSource.map { edk -> edk.entityKeyId }
                    )
                    it.setObject(1, state.entitySetId)
                    it.setArray(2, entityKeyIdsArr)
                    it.setObject(3, state.entitySetId)
                    it.setArray(4, entityKeyIdsArr)
                    it.setObject(5, state.entitySetId)
                    it.setArray(6, entityKeyIdsArr)
                }) {
                    DataEdgeKey(
                        ResultSetAdapters.srcEntityDataKey(it),
                        ResultSetAdapters.dstEntityDataKey(it),
                        ResultSetAdapters.edgeEntityDataKey(it)
                    )
                }
            }.toSet()
    }

    @SuppressFBWarnings(
        value = ["RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE"],
        justification = "This is a bug with spotbugs bytecode parsing for lateinit var."
    )
    private fun deleteEntities(entityDataKeys: Set<EntityDataKey>): Int {
        val entitySetIdToPartitions = getEntitySetPartitions(entityDataKeys)

        val (deleteFromDataSql, deleteFromIdsSql) = if (isHardDelete()) {
            HARD_DELETE_FROM_DATA_SQL to HARD_DELETE_FROM_IDS_SQL
        } else {
            SOFT_DELETE_FROM_DATA_SQL to SOFT_DELETE_FROM_IDS_SQL
        }

        val version = -System.currentTimeMillis()

        if (isHardDelete()) {
            val esIdToBinaryPts = getBinaryPropertiesOfEntitySets(entitySetIdToPartitions.keys)
            if (esIdToBinaryPts.isNotEmpty()) {
                val entitySetIdToPartitionToIds =
                    entityDataKeys.groupBy { it.entitySetId }.mapValues { (entitySetId, edks) ->
                        val partitions = entitySetIdToPartitions.getValue(entitySetId).toList()
                        edks.map { it.entityKeyId }.groupBy { getPartition(it, partitions) }
                    }
                deletePropertyOfEntityFromS3(
                    esIdToBinaryPts.keys,
                    esIdToBinaryPts.keys.flatMap {
                        entitySetIdToPartitionToIds.getValue(it).flatMap { (_, ids) -> ids }
                    },
                    esIdToBinaryPts.flatMap { it.value }
                )
            }
        }

        return entityDataKeys.groupBy { lateInitProvider.resolver.getDataSourceName(it.entitySetId) }
            .map { (dataSourceName, entityDataKeysForDataSource) ->
                val dataHds = lateInitProvider.resolver.getDataSource(dataSourceName)
                val idsHds = lateInitProvider.resolver.getDefaultDataSource()
                val entitySetIdToPartitionToIds = entityDataKeysForDataSource
                    .groupBy { edkForDataSource -> edkForDataSource.entitySetId }
                    .mapValues { (entitySetId, edks) ->
                        val partitions = entitySetIdToPartitions.getValue(entitySetId).toList()
                        edks.map { edk -> edk.entityKeyId }.groupBy { id -> getPartition(id, partitions) }
                    }

                dataHds.connection.use {
                    it.prepareStatement(deleteFromDataSql).use { ps ->

                        entitySetIdToPartitionToIds.forEach { (entitySetId, partitionToIds) ->
                            partitionToIds.forEach { (partition, ids) ->
                                bindEntityDelete(ps, entitySetId, partition, ids, version)
                            }
                        }
                        ps.executeBatch()
                    }
                }.sum() + idsHds.connection.use {
                    it.prepareStatement(deleteFromIdsSql).use { ps ->
                        entitySetIdToPartitionToIds.forEach { (entitySetId, partitionToIds) ->
                            partitionToIds.forEach { (partition, ids) ->
                                bindEntityDelete(ps, entitySetId, partition, ids, version)
                            }
                        }
                        ps.executeBatch()
                    }.sum()
                }
            }.sum()
    }

    @JsonIgnore
    private fun getBinaryPropertiesOfEntitySets(entitySetIds: Set<UUID>): Map<UUID, List<UUID>> {
        val entitySetToEntityType = entitySets.executeOnKeys(entitySetIds, GetEntityTypeFromEntitySetEntryProcessor())
        val entityTypeToProperties =
            entityTypes.executeOnKeys(entitySetToEntityType.values.toSet(), GetPropertiesFromEntityTypeEntryProcessor())
        val binaryPropertyTypeIds = propertyTypes.getAll(entityTypeToProperties.flatMap { it.value }.toSet()).values
            .filter { it.datatype == EdmPrimitiveTypeKind.Binary }.mapTo(mutableSetOf()) { it.id }

        return entitySetToEntityType.mapNotNull { (entitySetId, entityTypeId) ->
            val binaryPts = entityTypeToProperties[entityTypeId]?.filter { binaryPropertyTypeIds.contains(it) }
            if (binaryPts == null) null else entitySetId to binaryPts
        }.toMap()
    }

    @JsonIgnore
    private fun getEntitySetPartitions(entityDataKeys: Set<EntityDataKey>): Map<UUID, Iterable<Int>> {
        val entitySetIds = entityDataKeys.mapTo(mutableSetOf()) { it.entitySetId }
        val esToPartitions = entitySets.executeOnKeys(entitySetIds, GetPartitionsFromEntitySetEntryProcessor())

        return entitySetIds.associateWith {
            esToPartitions[it]?.let { partitions ->
                if (partitions.isNotEmpty()) {
                    return@associateWith partitions
                }
            }

            ALL_PARTITIONS
        }
    }

    private fun bindEntityDelete(
        ps: PreparedStatement,
        entitySetId: UUID,
        partition: Int,
        ids: Collection<UUID>,
        version: Long
    ) {
        var index = 1
        if (!isHardDelete()) {
            ps.setLong(index++, version)
            ps.setLong(index++, version)
            ps.setLong(index++, version)
        }
        ps.setObject(index++, entitySetId)
        ps.setInt(index++, partition)
        ps.setArray(index, PostgresArrays.createUuidArray(ps.connection, ids))

        ps.addBatch()
    }

    private fun deleteEdges(edgeBatch: Set<DataEdgeKey>) {
        lateInitProvider.dataGraphService.deleteAssociations(edgeBatch, state.deleteType)
    }

    private fun bindEdgeDelete(ps: PreparedStatement, edk: EntityDataKey, version: Long) {
        var index = 1
        if (!isHardDelete()) {
            ps.setLong(index++, version)
            ps.setLong(index++, version)
        }
        ps.setObject(index++, edk.entitySetId)
        ps.setObject(index, edk.entityKeyId)
        ps.addBatch()
    }

    @SuppressFBWarnings(
        value = ["RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE"],
        justification = "This is a bug with spotbugs bytecode parsing for lateinit var."
    )
    //TODO: entity key ids should be paired with their entity set ids :-/
    @JsonIgnore
    private fun deletePropertyOfEntityFromS3(
        entitySetIds: Collection<UUID>,
        entityKeyIds: Collection<UUID>,
        propertyTypeIds: Collection<UUID>
    ) {

        val s3Keys =
            entitySetIds
                .groupBy(lateInitProvider.resolver::getDataSourceName)
                .flatMap { (dataSourceName, entitySetIdsForDataSource) ->
                    val hds = lateInitProvider.resolver.getDataSource(dataSourceName)
                    BasePostgresIterable<String>(
                        PreparedStatementHolderSupplier(hds, selectEntitiesTextProperties, FETCH_SIZE) { ps ->
                            val connection = ps.connection
                            val entitySetIdsArr = PostgresArrays.createUuidArray(connection, entitySetIdsForDataSource)
                            val propertyTypeIdsArr = PostgresArrays.createUuidArray(connection, propertyTypeIds)
                            val entityKeyIdsArr = PostgresArrays.createUuidArray(connection, entityKeyIds)
                            ps.setArray(1, entitySetIdsArr)
                            ps.setArray(2, propertyTypeIdsArr)
                            ps.setArray(3, entityKeyIdsArr)
                        }
                    ) { it.getString(getMergedDataColumnName(PostgresDatatype.TEXT)) }
                }


        if (s3Keys.isNotEmpty()) {
            try {
                lateInitProvider.byteBlobDataManager.deleteObjects(s3Keys)
            } catch (e: Exception) {
                logger.error(
                    "Unable to delete object from s3 for entity sets {} with ids {}",
                    entitySetIds,
                    entityKeyIds,
                    e
                )
            }
        }
    }

    override fun setLateInitProvider(lateInitProvider: LateInitProvider) {
        this.lateInitProvider = lateInitProvider
    }

    @JsonIgnore
    override fun setHazelcastInstance(hazelcastInstance: HazelcastInstance) {
        super.setHazelcastInstance(hazelcastInstance)
    }

    @JsonIgnore
    private fun isHardDelete(): Boolean {
        return state.deleteType == DeleteType.Hard
    }

    @JsonIgnore
    private fun excludeClearedIfSoftDeleteSql(): String {
        val operator = if (isHardDelete()) "<>" else ">"
        return "AND ${VERSION.name} $operator 0"
    }

    /**
     * PreparedStatement bind order:
     *
     * 1) entitySetId
     */
    @JsonIgnore
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
    @JsonIgnore
    private fun getIdsBatchSql(): String {
        return """
            SELECT ${ENTITY_SET_ID.name}, ${ID.name}
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
    @JsonIgnore
    private fun getEdgesBatchSql(): String {
        val entityMatches = listOf(
            SRC_ENTITY_SET_ID to SRC_ENTITY_KEY_ID,
            EDGE_ENTITY_SET_ID to EDGE_ENTITY_KEY_ID,
            DST_ENTITY_SET_ID to DST_ENTITY_KEY_ID
        ).joinToString(" OR ") { (entitySetIdCol, entityKeyIdCol) ->
            "( ${entitySetIdCol.name} = ? AND ${entityKeyIdCol.name} = ANY(?) )"
        }

        return """
            SELECT  ${SRC_ENTITY_SET_ID.name}, ${SRC_ENTITY_KEY_ID.name},
                    ${DST_ENTITY_SET_ID.name}, ${DST_ENTITY_KEY_ID.name},
                    ${EDGE_ENTITY_SET_ID.name}, ${EDGE_ENTITY_KEY_ID.name}
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
    @JsonIgnore
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
    @JsonIgnore
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
    @JsonIgnore
    private val HARD_DELETE_FROM_IDS_SQL = zeroVersionsForEntitiesInEntitySet

    /**
     * 1. version
     * 2. version
     * 3. version
     * 4. entitySetId
     * 5. partition
     * 6. entityKeyIds
     */
    @JsonIgnore
    private val SOFT_DELETE_FROM_IDS_SQL = updateVersionsForEntitiesInEntitySet


    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        if (!super.equals(other)) return false

        other as DataDeletionJob

        return true
    }

    override fun hashCode(): Int {
        return super.hashCode()
    }


}
