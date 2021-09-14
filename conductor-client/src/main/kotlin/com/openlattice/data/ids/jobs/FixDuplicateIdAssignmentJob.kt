package com.openlattice.data.ids.jobs

import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.module.kotlin.readValue
import com.geekbeast.rhizome.jobs.AbstractDistributedJob
import com.geekbeast.rhizome.jobs.JobStatus
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.data.EntityKey
import com.openlattice.graph.partioning.REPARTITION_DATA_COLUMNS
import com.openlattice.graph.partioning.REPARTITION_EDGES_COLUMNS
import com.openlattice.hazelcast.serializers.decorators.IdGenerationAware
import com.openlattice.hazelcast.serializers.decorators.MetastoreAware
import com.openlattice.ids.HazelcastIdGenerationService
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.*
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.util.*

const val BATCH_SIZE = 128000

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class FixDuplicateIdAssignmentJob(
        state: FixDuplicateIdAssignmentJobState
) : AbstractDistributedJob<Long, FixDuplicateIdAssignmentJobState>(state), MetastoreAware, IdGenerationAware {
    companion object {
        @Transient
        private val mapper = ObjectMappers.newJsonMapper()
    }

    @JsonCreator
    constructor(
            id: UUID?,
            taskId: Long?,
            status: JobStatus,
            progress: Byte,
            hasWorkRemaining: Boolean,
            result: Long?,
            state: FixDuplicateIdAssignmentJobState
    ) : this(state) {
        initialize(id, taskId, status, progress, hasWorkRemaining, result)
    }

    @Transient
    private lateinit var hds: HikariDataSource

    @Transient
    private lateinit var idService: HazelcastIdGenerationService

    override fun processNextBatch() {
        /**
         * Process ids in batches.
         */
        val ids = BasePostgresIterable(PreparedStatementHolderSupplier(hds, SELECT_NEXT_IDS) { ps ->
            ps.setObject(1, state.id)
        }) { rs ->
            ResultSetAdapters.id(rs)
        }.toList()

        val entityKeysAndIds = BasePostgresIterable(PreparedStatementHolderSupplier(hds, SELECT_NEXT_DUPLICATES) { ps ->
            ps.setArray(1, PostgresArrays.createUuidArray(ps.connection, ids))
        }) { rs ->
            ResultSetAdapters.id(rs) to mapper.readValue<List<EntityKey>>(rs.getString(DUPLICATES_FIELD))
        }.toMap()

        entityKeysAndIds.forEach { (id, entityKeys) ->
            val entitySetAppearances = entityKeys.groupingBy { it.entitySetId }.eachCount()
            val onlyOnce = entitySetAppearances.filterValues { it == 1 }
            val multiple = entitySetAppearances.filterValues { it > 1 }

            val canBeRepaired = entityKeys.filter { onlyOnce.containsKey(it.entitySetId) }
            val collisions = entityKeys.filter { multiple.containsKey(it.entitySetId) }

            /**
             * Each entity set appears only once. We will assign new ids to the entity keys and update ids, data, edges
             */
            if (canBeRepaired.isNotEmpty()) {
                markAsNeedingIndexing(assignNewIdsAndMoveData(id, canBeRepaired.subList(1, entityKeys.size)))
            }

            /**
             * We have two entity keys in the same entity set assigned to the same id. This is
             * extra special bad and we do not know how to handle without manual intervention
             */
            if (collisions.isNotEmpty()) {
                logger.error("Woe is us, we will have to manually address.")
                recordCollision(id, entityKeys)
            }
        }

        markAsNeedingIndexing(ids)

        state.idsProceessed += entityKeysAndIds.values.sumBy { it.size }
        state.id = ids.max() ?: throw IllegalStateException("Entity key ids cannot be empty.")
        hasWorkRemaining = (ids.size == BATCH_SIZE)
    }

    private fun markAsNeedingIndexing(ids: Collection<UUID>) {
        hds.connection.use { connection ->
            connection.prepareStatement(MARK_FOR_INDEXING).use { ps ->
                ps.setArray(1, PostgresArrays.createUuidArray(connection, ids))
                ps.executeUpdate()
            }
        }
    }

    private fun assignNewIdsAndMoveData(originalId: UUID, entityKeys: List<EntityKey>): Set<UUID> {
        val newIds = idService.getNextIds(entityKeys.size)
        val newIdsIter = newIds.iterator()
        hds.connection.use { connection ->
            connection.autoCommit = false

            val psSyncs = connection.prepareStatement(UPDATE_ID_ASSIGNMENT)
            val psData = connection.prepareStatement(MIGRATE_DATA)
            val psEdges = connection.prepareStatement(MIGRATE_EDGES)
            val psDst = connection.prepareStatement(UPDATE_E_DST)
            val psEdge = connection.prepareStatement(UPDATE_E_EDGE)

            entityKeys.forEachIndexed { index, entityKey ->
                val newId = newIdsIter.next()
                updateIdAssignment(psSyncs, entityKey, newId)
                migrate(psData, entityKey.entitySetId, originalId)
                migrate(psEdges, entityKey.entitySetId, originalId)
                update(psDst, newId, originalId)
                update(psEdge, newId, originalId)
            }
            listOf(psSyncs, psData, psEdges, psDst, psEdge).forEach { it.executeBatch() }

            connection.commit()
            connection.autoCommit = true
        }
        return newIds
    }

    private fun migrate(
            ps: PreparedStatement,
            originalId: UUID,
            entitySetId: UUID
    ) {
        bind(ps, entitySetId, originalId)
        ps.addBatch()
    }

    private fun update(
            ps: PreparedStatement,
            newId: UUID,
            originalId: UUID
    ) {
        bind(ps, newId, originalId)
        ps.addBatch()
    }

    private fun bind(ps: PreparedStatement, entitySetId: UUID, entityKeyId: UUID) {
        ps.setObject(1, entitySetId)
        ps.setObject(2, entityKeyId)
    }

    private fun updateIdAssignment(ps: PreparedStatement, entityKey: EntityKey, newId: UUID) {
        ps.setObject(1, newId)
        ps.setObject(2, entityKey.entitySetId)
        ps.setString(3, entityKey.entityId)
    }

    private fun recordCollision(id: UUID, entityKeys: List<EntityKey>) {
        hds.connection.use { connection ->
            connection.prepareStatement(INSERT_COLLISION).use { ps ->
                ps.setObject(1, id)
                entityKeys.forEach {
                    ps.setObject(2, it.entitySetId)
                    ps.setObject(3, it.entityId)
                    ps.addBatch()
                }
                ps.executeBatch()
            }
        }
    }

    override fun initialize() {
        state.idCount = getIdCount()
    }

    override fun updateProgress() {
        progress = ((100 * state.idsProceessed) / state.idCount).toByte()
    }

    private fun getIdCount(): Long {
        return hds.connection.use { connection ->
            connection.createStatement().use { statement ->
                val rs = statement.executeQuery(COUNT_SYNC_IDS)
                if (rs.next()) {
                    ResultSetAdapters.count(rs)
                } else {
                    throw IllegalStateException("Result set for counting ids cannot empty.")
                }
            }
        }
    }

    @JsonIgnore
    override fun setHikariDataSource(hds: HikariDataSource) {
        this.hds = hds
    }

    @JsonIgnore
    override fun setIdGenerationService(idService: HazelcastIdGenerationService) {
        this.idService = idService
    }
}

private const val DUPLICATES_FIELD = "duplicates"
private val COUNT_SYNC_IDS = """
SELECT count(*) FROM ${SYNC_IDS.name}
""".trimIndent()

/**
 * Returns the duplicates for a batch of ids.
 * 1. ids - array of uuids
 */
private val SELECT_NEXT_DUPLICATES = """
SELECT  ${ID.name}, 
        jsonb_agg(jsonb_build_object(
            '${SerializationConstants.ENTITY_SET_ID}', ${ENTITY_SET_ID.name},
            '${SerializationConstants.ENTITY_ID}',${ENTITY_ID.name})
        ) as $DUPLICATES_FIELD 
FROM ${SYNC_IDS.name} where ${ID.name} = ANY(?) 
GROUP BY ${ID.name}
HAVING count(${ENTITY_ID.name}) > 1
""".trimIndent()

/**
 * Returns the next batch of ids of size [BATCH_SIZE]. Proof: If a partition contains an element
 * smaller than the max id returned from the query then it would have been returned in the query instead of the max element.
 *
 * 1. id - uuid
 */
private val SELECT_NEXT_IDS = """
SELECT DISTINCT ${ID.name} FROM ${SYNC_IDS.name} WHERE ${ID.name} > ? LIMIT $BATCH_SIZE
""".trimIndent()

/**
 * 1. entity set
 * 2. id - uuid
 */
private val MIGRATE_DATA = """
WITH for_migration as (DELETE FROM ${DATA.name} WHERE ${ENTITY_SET_ID.name} = ? AND ${ID.name} = ? RETURNING *)
    INSERT INTO ${DATA.name} 
        SELECT $REPARTITION_DATA_COLUMNS 
        FROM for_migration
        INNER JOIN (select ${ID.name}  as ${ENTITY_SET_ID.name}, ${PARTITIONS.name} FROM ${ENTITY_SETS.name} ) as es 
        USING(${ENTITY_SET_ID.name})  
""".trimIndent()

/**
 * 1. entity set
 * 2. id - uuid
 */
private val MIGRATE_EDGES = """
WITH for_migration as (DELETE FROM ${E.name} WHERE ${SRC_ENTITY_SET_ID.name} = ? AND ${SRC_ENTITY_KEY_ID.name} = ? RETURNING *)
    INSERT INTO ${E.name} 
        SELECT $REPARTITION_EDGES_COLUMNS 
        FROM for_migration 
        INNER JOIN (select ${ID.name} as ${SRC_ENTITY_SET_ID.name}, ${PARTITIONS.name} FROM ${ENTITY_SETS.name} ) as es 
        USING(${SRC_ENTITY_SET_ID.name})  
""".trimIndent()

/**
 * 1. (new) dst_entity_key_id - uuid
 * 2. (old) dst_entity_key_id - uuid
 */
private val UPDATE_E_DST = """
UPDATE ${E.name} SET ${DST_ENTITY_KEY_ID.name} = ? WHERE ${DST_ENTITY_KEY_ID.name} = ? 
""".trimIndent()

/**
 * 1. (new) edge_entity_key_id - uuid
 * 2. (old) edge_entity_key_id - uuid
 */
private val UPDATE_E_EDGE = """
UPDATE ${E.name} SET ${EDGE_ENTITY_KEY_ID.name} = ? WHERE ${EDGE_ENTITY_KEY_ID.name} = ?    
""".trimIndent()

/**
 * Updates the assigned entity key id for an entity key.
 * 1. entity key id - uuid
 * 2. entity set id - uuid
 * 3. entity id - string
 */
private val UPDATE_ID_ASSIGNMENT = """
UPDATE ${SYNC_IDS.name} SET ${ID.name} = ? WHERE ${ENTITY_SET_ID.name} = ? AND ${ENTITY_ID.name} = ? 
""".trimIndent()

/**
 * Record the collisions we couldn't fix.
 * 1. id - uuid
 * 2. entity set id - uuid
 * 3. entity id - string
 */
private val INSERT_COLLISION = """
INSERT INTO ${COLLISIONS.name} (${ID_VALUE.name}, ${ENTITY_SET_ID.name}, ${ENTITY_ID.name}) VALUES (?,?,?) ON CONFLICT DO NOTHING
""".trimIndent()

/***
 * Mark entities for re-indexing.
 * 1. ids - uuid array
 */
private val MARK_FOR_INDEXING = """
UPDATE ${IDS.name} SET ${LAST_WRITE.name} = now() WHERE ${ID.name} = ANY(?)
""".trimIndent()