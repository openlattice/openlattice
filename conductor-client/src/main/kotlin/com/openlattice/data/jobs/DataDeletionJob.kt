package com.openlattice.data.jobs

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.geekbeast.rhizome.jobs.AbstractDistributedJob
import com.geekbeast.rhizome.jobs.JobStatus
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.openlattice.data.DataEdgeKey
import com.openlattice.data.DeleteType
import com.openlattice.data.EntityDataKey
import com.openlattice.data.WriteEvent
import com.openlattice.data.storage.*
import com.openlattice.data.storage.partitions.getPartition
import com.openlattice.edm.EntitySet
import com.openlattice.edm.processors.GetEntityTypeFromEntitySetEntryProcessor
import com.openlattice.edm.processors.GetPartitionsFromEntitySetEntryProcessor
import com.openlattice.edm.processors.GetPropertiesFromEntityTypeEntryProcessor
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.ioc.providers.LateInitAware
import com.openlattice.ioc.providers.LateInitProvider
import com.openlattice.linking.graph.PostgresLinkingQueryService
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresDatatype
import com.openlattice.postgres.PostgresTable.*
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
        logger.info("${state.totalToDelete} entities to be deleted")
    }

    override fun processNextBatch() {
        val entityDataKeys = getBatchOfEntityDataKeys()

        if (entityDataKeys.isEmpty()) {
            hasWorkRemaining = false
            publishJobState()
            return
        }

        // batch delete neighbors first. Only delete from state.entitySetId if there are no more neighbors to delete
        val neighborEntityDataKeys = getBatchOfNeighborEntityDataKeys(entityDataKeys.map { it.entityKeyId }.toSet())
        if (neighborEntityDataKeys.isNotEmpty()) {
            deleteNeighborEntitiesAndEdges(neighborEntityDataKeys)
            return
        }

        logger.info("Processing data keys: {}", entityDataKeys)
        var edgeBatch = getBatchOfEdgesForIds(entityDataKeys)
        while (edgeBatch.isNotEmpty()) {
            logger.info("${state.deleteType} deleting edges and entities involving {}", edgeBatch)
            val edgeEdkBatch = edgeBatch.map { it.edge }.toSet()
            val deletedEntities = deleteEntities(edgeEdkBatch)
            val deletedEdges = deleteEdges(edgeBatch).numUpdates
            logger.info("Deleted $deletedEntities edge entities and $deletedEdges edges.")
            edgeBatch = getBatchOfEdgesForIds(entityDataKeys)
        }

        val deletedEntities = deleteEntities(entityDataKeys)
        logger.info("Deleted $deletedEntities from batch {}", entityDataKeys)
        state.numDeletes += deletedEntities
        cleanUpBatch(entityDataKeys)
        publishJobState()
    }

    override fun updateProgress() {
        if (state.totalToDelete > 0) {
            progress = ((100 * state.numDeletes) / state.totalToDelete).toByte()
        }
    }

    private fun getNeighborTotalToDelete(): Long {
        val neighborEntitySetIds = state.neighborSrcEntitySetIds + state.neighborDstEntitySetIds
        if (neighborEntitySetIds.isEmpty()) {
            return 0
        }

        val hds = lateInitProvider.resolver.getDefaultDataSource()
        return hds.connection.use { connection ->
            connection.prepareStatement(getNeighborIdsCountSql()).use { ps ->

                var index = 0
                if (state.neighborDstEntitySetIds.isNotEmpty()) {
                    ps.setArray(++index, PostgresArrays.createUuidArray(connection, state.neighborDstEntitySetIds))
                    ps.setObject(++index, state.entitySetId)
                    ps.setArray(++index, PostgresArrays.createUuidArray(connection, state.entityKeyIds))
                }

                if (state.neighborSrcEntitySetIds.isNotEmpty()) {
                    ps.setArray(++index, PostgresArrays.createUuidArray(connection, state.neighborSrcEntitySetIds))
                    ps.setObject(++index, state.entitySetId)
                    ps.setArray(++index, PostgresArrays.createUuidArray(connection, state.entityKeyIds))
                }

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
    private fun getTotalToDelete(): Long {
        state.entityKeyIds?.let {
            return it.size.toLong() + getNeighborTotalToDelete()
        }
        val hds = lateInitProvider.resolver.resolve(state.entitySetId)
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
    private fun deleteNeighborEntitiesAndEdges(entityDataKeys: Set<EntityDataKey>) {

        logger.info("Processing neighbor entity data keys of batch size ${entityDataKeys.size}")

        var edgeBatch = getBatchOfNeighborEdges(entityDataKeys)
        while (edgeBatch.isNotEmpty()) {
            logger.info("${state.deleteType} deleting neighbor edge entities and edges of size ${edgeBatch.size}")
            val edgeEdkBatch = edgeBatch.map { it.edge }.toSet()
            val deletedEntities = deleteEntities(edgeEdkBatch)
            val deletedEdges = deleteEdges(edgeBatch).numUpdates
            logger.info("Deleted $deletedEntities neighbor edge entities and $deletedEdges edges")
            edgeBatch = getBatchOfNeighborEdges(entityDataKeys)
        }

        val deletedEntities = deleteEntities(entityDataKeys)
        logger.info("Deleted $deletedEntities neighbor entities from batch size ${entityDataKeys.size}")
        state.numDeletes += deletedEntities
    }

    /**
     * Returns edges with either src or dst matching the given entity data keys
     * @param entityDataKeys: Can be from multiple entity sets
     */
    private fun getBatchOfNeighborEdges(entityDataKeys: Set<EntityDataKey>): Set<DataEdgeKey> {

        return entityDataKeys
                .groupBy { it.entitySetId }
                .flatMap { (entitySetId, entityDataKeys) ->
                    val dataSourceName = lateInitProvider.resolver.getDataSourceName(entitySetId)
                    val hds = lateInitProvider.resolver.getDataSource(dataSourceName)

                    BasePostgresIterable(PreparedStatementHolderSupplier(hds, getNeighborEdgesBatchSql()) { ps ->
                        val entityKeyIdsArr = PostgresArrays.createUuidArray(ps.connection, entityDataKeys.map { it.entityKeyId })

                        ps.setObject(1, entitySetId)
                        ps.setArray(2, entityKeyIdsArr)
                        ps.setObject(3, entitySetId)
                        ps.setObject(4, entityKeyIdsArr)
                    }) {
                        DataEdgeKey(
                                ResultSetAdapters.srcEntityDataKey(it),
                                ResultSetAdapters.dstEntityDataKey(it),
                                ResultSetAdapters.edgeEntityDataKey(it)
                        )
                    }
                }.toSet()
    }

    /**
     * Returns entity data keys of neighbors connected to the specified entity key ids
     *
     * @param entityKeyIds These are non-empty and from the same entity set id
     */
    @JsonIgnore
    private fun getBatchOfNeighborEntityDataKeys(entityKeyIds: Set<UUID>): Set<EntityDataKey> {
        val neighborEntitySets = state.neighborDstEntitySetIds + state.neighborSrcEntitySetIds
        if (neighborEntitySets.isEmpty()) return setOf()

        val hds = lateInitProvider.resolver.getDefaultDataSource()

        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, getNeighborIdsBatchSql()) {
            var index = 0

            if (state.neighborDstEntitySetIds.isNotEmpty()) {
                it.setArray(++index, PostgresArrays.createUuidArray(it.connection, state.neighborDstEntitySetIds))
                it.setObject(++index, state.entitySetId)
                it.setArray(++index, PostgresArrays.createUuidArray(it.connection, entityKeyIds))
            }

            if (state.neighborSrcEntitySetIds.isNotEmpty()) {
                it.setArray(++index, PostgresArrays.createUuidArray(it.connection, state.neighborSrcEntitySetIds))
                it.setObject(++index, state.entitySetId)
                it.setArray(++index, PostgresArrays.createUuidArray(it.connection, entityKeyIds))
            }
        }) {
            ResultSetAdapters.entityDataKey(it)
        }.toSet()
    }

    @JsonIgnore
    private fun getBatchOfEntityDataKeys(): Set<EntityDataKey> {
        state.entityKeyIds?.let { entityKeyIds ->
            return entityKeyIds.take(BATCH_SIZE).mapTo(mutableSetOf()) { EntityDataKey(state.entitySetId, it) }
        }

        val hds = lateInitProvider.resolver.resolve(state.entitySetId)

        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, getIdsBatchSql()) {
            it.setObject(1, state.entitySetId)
            it.setArray(2, PostgresArrays.createIntArray(it.connection, state.partitions.getValue(state.entitySetId)))
        }) {
            ResultSetAdapters.entityDataKey(it)
        }.toSet()
    }

    private fun cleanUpBatch(entityDataKeys: Set<EntityDataKey>) {
        val entityKeyIds = entityDataKeys.mapTo(mutableSetOf()) { it.entityKeyId }
        //This is just affecting linking so we use default data source by design.
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
//                val idsHds = lateInitProvider.resolver.getDefaultDataSource()
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
                        }.sum() + it.prepareStatement(deleteFromIdsSql).use { ps ->
                            entitySetIdToPartitionToIds.forEach { (entitySetId, partitionToIds) ->
                                partitionToIds.forEach { (partition, ids) ->
                                    bindEntityDelete(ps, entitySetId, partition, ids, version)
                                }
                            }
                            ps.executeBatch().sum()
                        }
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

    private fun deleteEdges(edgeBatch: Set<DataEdgeKey>): WriteEvent = lateInitProvider.dataGraphService.deleteAssociations(
            edgeBatch,
            state.deleteType
    )

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
     * PreparedStatement bind order
     *
     * 1) dstEntitySetIds
     * 2) entitySetId
     * 3) entityKeyIds
     * 4) srcEntitySetIds
     * 5) entitySetId
     * 6) entityKeyIds
     */
    private fun getNeighborIdsCountSql(): String {
        val dstFilter = if (state.neighborDstEntitySetIds.isEmpty()) "" else """
            (${DST_ENTITY_SET_ID.name} = ANY(?) AND ${SRC_ENTITY_SET_ID.name} = ? AND ${SRC_ENTITY_KEY_ID.name} = ANY(?))
        """.trimIndent()

        val srcFilter = if (state.neighborSrcEntitySetIds.isEmpty()) "" else """
            (${SRC_ENTITY_SET_ID.name} = ANY(?) AND ${DST_ENTITY_SET_ID.name} = ? AND ${DST_ENTITY_KEY_ID.name} = ANY(?))
        """.trimIndent()

        val filter = listOf(dstFilter, srcFilter).filter { it.isNotEmpty() }.joinToString( " OR ")

        return """
            SELECT COUNT(*)
            FROM ${E.name}
            WHERE $filter
        """.trimIndent()
    }

    /**
     * PreparedStatement bind order:
     * 1) entitySetId
     * 2) neighborEntityKeyIds
     * 3) entitySetId
     * 4) neighborEntityKeyIds
     */

    private fun getNeighborEdgesBatchSql(): String {
        return """
            SELECT ${SRC_ENTITY_SET_ID.name}, ${SRC_ENTITY_KEY_ID.name},
                   ${DST_ENTITY_SET_ID.name}, ${DST_ENTITY_KEY_ID.name},
                   ${EDGE_ENTITY_SET_ID.name},${EDGE_ENTITY_KEY_ID.name}
            FROM ${E.name}
            WHERE (${DST_ENTITY_SET_ID.name} = ? AND ${DST_ENTITY_KEY_ID.name} = ANY(?))
               OR (${SRC_ENTITY_SET_ID.name} = ? AND ${SRC_ENTITY_KEY_ID.name} = ANY(?))
            ${excludeClearedIfSoftDeleteSql()}
            LIMIT $BATCH_SIZE
        """.trimIndent()
    }

    /**
     * PreparedStatement bind order:
     *
     * 1) dstEntitySetIds
     * 2) entitySetId
     * 3) entityKeyIds
     * 4) srcEntitySetIds
     * 5) entitySetId
     * 6) entityKeyIds
     */
    @JsonIgnore
    private fun getNeighborIdsBatchSql(): String {
        val dstSql = if (state.neighborDstEntitySetIds.isEmpty()) "" else """
            SELECT ${DST_ENTITY_SET_ID.name} as $ENTITY_SET_ID_FIELD, ${DST_ENTITY_KEY_ID.name} as $ID_FIELD
            FROM ${E.name}
            WHERE ${DST_ENTITY_SET_ID.name} = ANY(?) AND ${SRC_ENTITY_SET_ID.name} = ? AND ${SRC_ENTITY_KEY_ID.name} = ANY(?)
            ${excludeClearedIfSoftDeleteSql()}
        """.trimIndent()

        val srcSql = if (state.neighborSrcEntitySetIds.isEmpty()) "" else """
            SELECT ${SRC_ENTITY_SET_ID.name} as $ENTITY_SET_ID_FIELD, ${SRC_ENTITY_KEY_ID.name} as $ID_FIELD
            FROM ${E.name}
            WHERE ${SRC_ENTITY_SET_ID.name} = ANY(?) AND ${DST_ENTITY_SET_ID.name} = ? AND ${DST_ENTITY_KEY_ID.name} = ANY(?)
            ${excludeClearedIfSoftDeleteSql()}
        """.trimIndent()

        return listOf(dstSql, srcSql).filter { it.isNotEmpty() }.joinToString(" UNION ", postfix = "LIMIT $BATCH_SIZE")
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
