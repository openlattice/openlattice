package com.openlattice.data.storage.partitions

import com.google.common.base.Preconditions.checkArgument
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.COUNT
import com.openlattice.postgres.PostgresColumn.PARTITION
import com.openlattice.postgres.PostgresMaterializedViews.Companion.PARTITION_COUNTS
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PartitionManager(private val hds: HikariDataSource, partitions: Int) {
    private var partitionList = mutableListOf<Int>()

    init {
        createMaterializedViewIfNotExists()
        setPartitions(partitions)
    }


    fun setPartitions(partitions: Int) {
        checkArgument(partitions > partitionList.size, "Only support increasing number of partitions.")
        //TODO: Support decreasing number of partitions, but this is unlikely to be needed, since decreasing
        //number of citus partitions will automatically have the desired effect.
        partitionList.addAll(partitionList.size until partitions)
    }

    fun allocatePartitions(organizationId: UUID, partitionCount: Int) : List<Int> {
        checkArgument(
                partitionCount < partitionList.size,
                "Cannot request more partitions ($partitionCount) than exist (${partitionList.size}."
        )
        = getEmptiestPartitions(partitionCount)
    }


    fun repartition(organizationId: UUID) {
        //TO
    }

    private fun createMaterializedViewIfNotExists() {
        hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(PARTITION_COUNTS.createSql)
            }
        }
    }

    fun refreshMaterializedView() {
        hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(PARTITION_COUNTS.refreshSql)
            }
        }
    }

    fun getPartitionInformation() : Map<Int, Long> {
        return BasePostgresIterable(
                PreparedStatementHolderSupplier(hds, EMPTIEST_PARTITIONS) { ps ->
                    ps.setArray(1, PostgresArrays.createIntArray(ps.connection, partitionList))
                    ps.setObject(2, desiredPartitions)
                }) { it.getInt(PARTITION.name) to it.getLong(COUNT) }
    }

    private fun getEmptiestPartitions(desiredPartitions: Int): PostgresIterable<Pair<Int, Long>> {
        //A quick note is that the partitions materialized view shouldn't be turned into a distributed table
        //with out addressing the interaction of the order by and limit clausees.
        return BasePostgresIterable(
                PreparedStatementHolderSupplier(hds, EMPTIEST_PARTITIONS) { ps ->
                    ps.setArray(1, PostgresArrays.createIntArray(ps.connection, partitionList))
                    ps.setObject(2, desiredPartitions)
                }) { it.getInt(PARTITION.name) to it.getLong(COUNT) }
    }

}


private val ALL_PARTITIONS = "SELECT ${PARTITION.name}, COALESCE(count,0) as partition_count FROM (SELECT unnest(?) as ${PARTITION.name}) as partitions LEFT JOIN ${PARTITION_COUNTS.name} USING (${PARTITION.name}) ORDER BY partition_count ASC "
private val EMPTIEST_PARTITIONS = "$ALL_PARTITIONS LIMIT ?"
