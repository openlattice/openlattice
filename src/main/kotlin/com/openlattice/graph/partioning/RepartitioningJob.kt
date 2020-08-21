package com.openlattice.graph.partioning

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.geekbeast.rhizome.jobs.AbstractDistributedJob
import com.geekbeast.rhizome.jobs.JobStatus
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.openlattice.data.storage.getPartitioningSelector
import com.openlattice.edm.EntitySet
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.serializers.decorators.MetastoreAware
import com.openlattice.postgres.DataTables.*
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresTable.*
import com.openlattice.postgres.PostgresTableDefinition
import com.openlattice.postgres.ResultSetAdapters
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.util.*

/**
 * Background job for re-partitioning data in an entity set.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class RepartitioningJob(
        state: RepartitioningJobState
) : AbstractDistributedJob<Long, RepartitioningJobState>(state), MetastoreAware {
    @JsonCreator
    constructor(
            id: UUID?,
            taskId: Long?,
            status: JobStatus,
            progress: Byte,
            hasWorkRemaining: Boolean,
            result: Long?,
            state: RepartitioningJobState,
            phase: RepartitioningPhase
    ) : this(state) {
        initialize(id, taskId, status, progress, hasWorkRemaining, result)
        this.phase = phase
    }

    constructor(
            entitySetId: UUID,
            oldPartitions: List<Int>,
            newPartitions: Set<Int>
    ) : this(RepartitioningJobState(entitySetId, oldPartitions, newPartitions))

    override val resumable: Boolean = true

    var phase: RepartitioningPhase = RepartitioningPhase.POPULATE
        private set

    @Transient
    private lateinit var hds: HikariDataSource

    @Transient
    private lateinit var entitySets: IMap<UUID, EntitySet>

    private val currentlyMigratingPartition: Int
        get() = state.oldPartitions[state.currentlyMigratingPartitionIndex]

    @JsonIgnore
    override fun setHazelcastInstance(hazelcastInstance: HazelcastInstance) {
        super.setHazelcastInstance(hazelcastInstance)
        this.entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)
    }

    @JsonIgnore
    override fun setHikariDataSource(hds: HikariDataSource) {
        this.hds = hds
    }

    override fun initialize() {
        state.needsMigrationCount = getNeedsMigrationCount()
    }

    override fun processNextBatch() {
        if (state.needsMigrationCount == 0L) {
            if (phase == RepartitioningPhase.POPULATE) {
                setPartitions(state.entitySetId, state.newPartitions)
            }

            hasWorkRemaining = false
            return
        }

        /**
         * Do an INSERT INTO ... SELECT FROM to re-partition the data.
         *
         * Entity key id dependent operations will not see data, until data has been inserted to the appropriate partition.
         */

        state.repartitionCount += repartition(REPARTITION_DATA_SQL)
        state.repartitionCount += repartition(REPARTITION_IDS_SQL)
        state.repartitionCount += repartition(REPARTITION_EDGES_SQL)

        /**
         * Phase 1
         * Delete data whose partition doesn't match its computed partition.
         */
        if (phase == RepartitioningPhase.FINALIZE) {
            state.deleteCount += delete(DELETE_DATA_SQL)
            state.deleteCount += delete(DELETE_IDS_SQL)
            state.deleteCount += delete(DELETE_EDGES_SQL)
        }

        result = state.repartitionCount + state.deleteCount
        hasWorkRemaining = (++state.currentlyMigratingPartitionIndex < state.oldPartitions.size)

        //TODO: Consider adding completion hook to distributable jobs framework
        //Once we are done, set the partitions.
        if (!hasWorkRemaining && phase == RepartitioningPhase.POPULATE) {
            //First, we save job progress with the latest job status so that the job can safely be resumed.
            state.currentlyMigratingPartitionIndex = 0
            hasWorkRemaining = true
            phase = RepartitioningPhase.FINALIZE
            try {
                //Make sure we pull any status updates, in case job has been canceled or pause
                updateJobStatus()
            } finally {
                //Even if something goes wrong with pulling job status let's save the state we are in.
                publishJobState()
            }

            setPartitions(state.entitySetId, state.newPartitions)
            //The 4*getCount was an estimate, we remove estimate and addin updated value.
            state.needsMigrationCount /= 2
            state.needsMigrationCount += getNeedsMigrationCount()
            //Another publish to save needsMigrationCount
            publishJobState()
        }
    }

    private fun delete(deleteSql: String): Long = hds.connection.use { connection ->
        try {
            connection.prepareStatement(deleteSql).use { deleteData ->
                bind(deleteData)
                logger.info(deleteData.toString())
                deleteData.executeLargeUpdate()
            }
        } catch (ex: Exception) {
            abort { "Job $id terminated: ${ex.message}" }
            throw ex
        } finally {
            publishJobState()
        }
    }

    override fun updateProgress() {
        progress = if (state.needsMigrationCount == 0L) {
            0
        } else {
            ((100 * (state.repartitionCount + state.deleteCount)) / state.needsMigrationCount).toByte()
        }
    }

    private fun repartition(repartitionSql: String): Long = hds.connection.use { connection ->
        updateProgress()
        try {
            connection.prepareStatement(repartitionSql).use { repartitionData ->
                bind(repartitionData)
                repartitionData.executeLargeUpdate()
            }
        } catch (ex: Exception) {
            abort { "Job $id terminated: ${ex.message}" }
            throw ex
        } finally {
            publishJobState()
        }
    }

    private fun getCount(countSql: String, partition: Int): Long = hds.connection.use { connection ->
        try {
            connection.prepareStatement(countSql).use { countQuery ->
                bind(countQuery, partition)
                val rs = countQuery.executeQuery()
                rs.next()
                ResultSetAdapters.count(rs)
            }
        } catch (ex: Exception) {
            abort { "Job $id terminated: ${ex.message}" }
            throw ex
        } finally {
            publishJobState()
        }
    }

    private fun getNeedsMigrationCount(): Long = 3 * state.oldPartitions.fold(0L) { count, partition ->
        count + getCount(idsNeedingMigrationCountSql, partition) +
                getCount(dataNeedingMigrationCountSql, partition) +
                getCount(edgesNeedingMigrationCountSql, partition)
    }


    private fun bind(ps: PreparedStatement, partition: Int = currentlyMigratingPartition) {
        ps.setObject(1, state.entitySetId)
        ps.setArray(2, PostgresArrays.createIntArray(ps.connection, state.newPartitions))
        ps.setInt(3, partition)
        ps.setObject(4, state.entitySetId)
    }

    private fun setPartitions(entitySetId: UUID, partitions: Set<Int>) {
        require(entitySets.containsKey(entitySetId)) {
            "Entity set $entitySetId not found"
        }

        entitySets.executeOnKey<Any>(entitySetId) {
            val v = it.value
            if (v != null) {
                v.setPartitions(partitions)
                it.setValue(v)
            }
            null
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is RepartitioningJob) return false
        if (!super.equals(other)) return false

        if (phase != other.phase) return false

        return true
    }

    override fun hashCode(): Int {
        var result = super.hashCode()
        result = 31 * result + phase.hashCode()
        return result
    }

}

private val REPARTITION_SELECTOR = getPartitioningSelector(ID)
private val REPARTITION_SELECTOR_E = getPartitioningSelector(SRC_ENTITY_KEY_ID)

private fun buildRepartitionColumns(ptd: PostgresTableDefinition): String {
    val selector = when (ptd) {
        E -> REPARTITION_SELECTOR_E
        else -> REPARTITION_SELECTOR
    }
    return ptd.columns.joinToString(",") { if (it == PARTITION) selector else it.name }
}

/**
 * Counts ids that need migrating on a single partition. We do one partition at a time to be able to re-use same
 * bind order across all queries in this class.
 * 1. entity set id
 * 2. partitions (array)
 * 3. partition
 */
private val idsNeedingMigrationCountSql = """
    SELECT count(*) 
    FROM ${IDS.name} INNER JOIN (select ? as ${ENTITY_SET_ID.name},? as ${PARTITIONS.name} ) as es
    USING ( ${ENTITY_SET_ID.name} )
    WHERE ${PARTITION.name} = ? AND ${PARTITION.name}!=$REPARTITION_SELECTOR
""".trimIndent()

/**
 * Counts data rows that need migrating on a single partition. We do one partition at a time to be able to re-use same
 * bind order across all queries in this class.
 * 1. entity set id
 * 2. partitions (array)
 * 3. partition
 */
private val dataNeedingMigrationCountSql = """
    SELECT count(*) 
    FROM ${DATA.name} INNER JOIN (select ? as ${ENTITY_SET_ID.name},? as ${PARTITIONS.name} ) as es 
    USING ( ${ENTITY_SET_ID.name} )
    WHERE ${PARTITION.name} = ? AND ${PARTITION.name}!=$REPARTITION_SELECTOR
""".trimIndent()

/**
 * Counts edges that need migrating on a single partition. We do one partition at a time to be able to re-use same
 * bind order across all queries in this class.
 * 1. entity set id
 * 2. partitions (array)
 * 3. partition
 */
private val edgesNeedingMigrationCountSql = """
    SELECT count(*)
    FROM ${E.name} INNER JOIN (select ? as ${SRC_ENTITY_SET_ID.name},? as ${PARTITIONS.name} ) as es
    USING (${SRC_ENTITY_SET_ID.name})
    WHERE ${PARTITION.name} = ? AND ${PARTITION.name}!=$REPARTITION_SELECTOR_E
""".trimIndent()

private fun latestSql(
        table: PostgresTableDefinition,
        column: PostgresColumnDefinition,
        comparison: PostgresColumnDefinition = column,
        whenExcludedGreater: PostgresColumnDefinition = column,
        otherwise: PostgresColumnDefinition = column
): String = "${column.name} = CASE WHEN EXCLUDED.${comparison.name} > ${table.name}.${comparison.name} THEN EXCLUDED.${whenExcludedGreater.name} ELSE ${table.name}.${otherwise.name} END"


private val REPARTITION_DATA_COLUMNS = buildRepartitionColumns(DATA)
private val REPARTITION_IDS_COLUMNS = buildRepartitionColumns(IDS)
private val REPARTITION_EDGES_COLUMNS = buildRepartitionColumns(E)

/**
 * Query for repartition a partition of data.
 *
 * 1. entity set id
 * 2. partitions (array)
 * 3. partition
 *
 * NOTE: We do not attempt to move data values on conflict. In theory, data is immutable and a conflict wouldn't impact the
 * actual content of the data columns, unless a hash collection had occured during a re-partition.
 * NOTE: We set origin_id based on version. This should be fine in 99.999% of cases as the latest version should have
 * the most up to date linkined. See note on  for exceptional case [REPARTITION_IDS_SQL]
 */
private val REPARTITION_DATA_SQL = """
INSERT INTO ${DATA.name} SELECT $REPARTITION_DATA_COLUMNS
    FROM ${DATA.name} INNER JOIN (select ? as ${ENTITY_SET_ID.name},? as ${PARTITIONS.name} ) as es 
    USING ( ${ENTITY_SET_ID.name} )
    WHERE ${PARTITION.name} = ? AND ${PARTITION.name}!=$REPARTITION_SELECTOR
    ON CONFLICT (${DATA.primaryKey.joinToString(",") { it.name }}) DO UPDATE SET
        ${latestSql(DATA, ORIGIN_ID, VERSION)},
        ${latestSql(DATA, VERSION, VERSION)},
        ${VERSIONS.name} = ARRAY( SELECT DISTINCT UNNEST(${DATA.name}.${VERSIONS.name} || EXCLUDED.${VERSIONS.name} ) ORDER BY 1  ),  
        ${latestSql(DATA, LAST_WRITE, VERSION)},
        ${latestSql(DATA, LAST_PROPAGATE, VERSION)}
""".trimIndent()

/**
 * Query for repartition a partition of ids.
 *
 * 1. entity set id
 * 2. partitions (array)
 * 3. partition
 *
 * NOTE: Using last_link for LINKING_ID in this query because a link can happen without triggering a version update.
 */
private val REPARTITION_IDS_SQL = """
INSERT INTO ${IDS.name} SELECT $REPARTITION_IDS_COLUMNS
    FROM ${IDS.name} INNER JOIN (select ? as ${ENTITY_SET_ID.name},? as ${PARTITIONS.name} ) as es 
    USING (${ENTITY_SET_ID.name})
    WHERE ${PARTITION.name} = ? AND ${PARTITION.name}!=$REPARTITION_SELECTOR
    ON CONFLICT (${IDS.primaryKey.joinToString(",") { it.name }}) DO UPDATE SET
        ${latestSql(IDS, LINKING_ID, LAST_LINK)}, 
        ${latestSql(IDS, VERSION, VERSION)},
        ${VERSIONS.name} = ARRAY( SELECT DISTINCT UNNEST(${IDS.name}.${VERSIONS.name} || EXCLUDED.${VERSIONS.name} ) ORDER BY 1  ),  
        ${latestSql(IDS, LAST_WRITE, VERSION)},
        ${latestSql(IDS, LAST_INDEX, VERSION)},
        ${latestSql(IDS, LAST_PROPAGATE, VERSION)},
        ${latestSql(IDS, LAST_MIGRATE, VERSION)},
        ${latestSql(IDS, LAST_LINK_INDEX, VERSION)}
""".trimIndent()

/**
 * Query for repartition a partition of edges.
 *
 * 1. entity set id
 * 2. partitions (array)
 * 3. partition
 *
 * NOTE: Using last_link for LINKING_ID in this query because a link can happen without triggering a version update.
 */
private val REPARTITION_EDGES_SQL = """
INSERT INTO ${E.name} SELECT $REPARTITION_EDGES_COLUMNS
    FROM ${E.name} INNER JOIN (select ? as ${SRC_ENTITY_SET_ID.name},? as ${PARTITIONS.name} ) as es
    USING (${SRC_ENTITY_SET_ID.name})
    WHERE ${PARTITION.name} = ? AND ${PARTITION.name}!=$REPARTITION_SELECTOR_E
    ON CONFLICT (${E.primaryKey.joinToString(",") { it.name }}) DO UPDATE SET
        ${latestSql(E, VERSION, VERSION)},
        ${VERSIONS.name} = ARRAY( SELECT DISTINCT UNNEST(${E.name}.${VERSIONS.name} || EXCLUDED.${VERSIONS.name} ) ORDER BY 1  )
""".trimIndent()

/**
 * Computes the actual partition and compares it to current partition. If partitions do not match deletes the row.
 * 1. entity set id
 * 2. partitions (array)
 * 3. partition
 * 4. entity set id
 *
 */
private val DELETE_DATA_SQL = """
DELETE FROM ${DATA.name} 
    USING (SELECT ${ID.name},${ENTITY_SET_ID.name},${PARTITION.name},${PARTITIONS.name} FROM ${DATA.name} INNER JOIN (select ? as ${ENTITY_SET_ID.name},? as ${PARTITIONS.name} ) as es USING (${ENTITY_SET_ID.name})) as to_be_deleted
    WHERE 
      ${DATA.name}.${PARTITION.name} = ?
      AND ${DATA.name}.${ENTITY_SET_ID.name} = ?
      AND ${DATA.name}.${PARTITION.name}!=${getPartitioningSelector(DATA.name + "." + ID.name)} 
      AND to_be_deleted.${ID.name} = ${DATA.name}.${ID.name} 
      AND to_be_deleted.${PARTITION.name} = ${DATA.name}.${PARTITION.name};  
""".trimIndent()

/**
 * Computes the actual partition and compares it to current partition. If partitions do not match deletes the row.
 * 1. entity set id
 * 2. partitions (array)
 * 3. partition
 * 4. entity set id
 *
 */
private val DELETE_IDS_SQL = """
DELETE FROM ${IDS.name} 
    USING (SELECT ${ID.name},${ENTITY_SET_ID.name},${PARTITION.name},${PARTITIONS.name} FROM ${IDS.name} INNER JOIN (select ? as ${ENTITY_SET_ID.name},? as ${PARTITIONS.name} ) as es USING (${ENTITY_SET_ID.name})) as to_be_deleted
    WHERE 
      ${IDS.name}.${PARTITION.name} = ?
      AND ${IDS.name}.${ENTITY_SET_ID.name} = ?
      AND ${IDS.name}.${PARTITION.name}!=${getPartitioningSelector(IDS.name + "." + ID.name)} 
      AND to_be_deleted.${ID.name} = ${IDS.name}.${ID.name} 
      AND to_be_deleted.${PARTITION.name} = ${IDS.name}.${PARTITION.name};  
""".trimIndent()

/**
 * Computes the actual partition and compares it to current partition. If partitions do not match deletes the row.
 *
 * 1. entity set id
 * 2. partitions (array)
 * 3. partition
 * 4. entity set id
 *
 */
private val DELETE_EDGES_SQL = """
DELETE FROM ${E.name} 
    USING (SELECT ${SRC_ENTITY_SET_ID.name},${SRC_ENTITY_KEY_ID.name},${PARTITION.name},${PARTITIONS.name} FROM ${E.name} INNER JOIN (select ? as ${SRC_ENTITY_SET_ID.name},? as ${PARTITIONS.name} ) as es USING (${SRC_ENTITY_SET_ID.name})) as to_be_deleted
    WHERE
      ${E.name}.${PARTITION.name} = ? 
      AND ${E.name}.${ENTITY_SET_ID.name} = ?
      AND ${E.name}.${PARTITION.name}!=${getPartitioningSelector(E.name + "." + SRC_ENTITY_KEY_ID.name)} 
      AND to_be_deleted.${SRC_ENTITY_SET_ID.name} = ${E.name}.${SRC_ENTITY_SET_ID.name} 
      AND to_be_deleted.${PARTITION.name} = ${E.name}.${PARTITION.name};
""".trimIndent()