package com.openlattice.graph.partioning

import com.geekbeast.rhizome.jobs.AbstractDistributedJob
import com.geekbeast.rhizome.jobs.DistributedJobState
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.E

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class RepartitioningJob(state: DistributedJobState) : AbstractDistributedJob<Long>(state) {
    override fun result(): Long? {
        TODO("Not yet implemented")
    }

    override fun next() {

        /**
         * Phase 1
         * Do a INSERT INTO ... SELECT FROM to re-partition the data.
         *
         * entity key id depenedent operations will not see data, until data has been inserted to the appropriate partition.
         *
         * Writes will likely be blocked
         */

        /**
         * Phase 2
         * Delete data whose partition doesn't match it's computed partition.
         */
    }

    override fun hasWorkRemaining(): Boolean {
        (state as RepartitioningJobState)
        return false
    }
}

private val REPARTITION_SELECTOR = "partitions[ 1 + ((array_length(partitions,1) + (('x'||right(${SRC_ENTITY_KEY_ID.name}::text,8))::bit(32)::int % array_length(partitions,1))) % array_length(partitions,1))]"
private val REPARTITION_COLUMNS = E.columns.joinToString(",") { if (it == PARTITION) REPARTITION_SELECTOR else it.name }
private val REPARTITION_SQL = """
INSERT INTO ${E.name} SELECT $REPARTITION_COLUMNS
    FROM ${E.name} INNER JOIN (select id as entity_set_id, partitions from entity_sets) as es 
    ON ${SRC_ENTITY_SET_ID.name} = es.entity_set_id
    WHERE entity_set_id = ? AND partition!=$REPARTITION_SELECTOR
""".trimIndent()
