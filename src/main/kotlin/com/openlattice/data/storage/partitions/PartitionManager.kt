package com.openlattice.data.storage.partitions

import com.google.common.base.Preconditions.checkArgument
import com.hazelcast.core.HazelcastInstance
import com.openlattice.edm.EntitySet
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.edm.types.processors.UpdateEntitySetMetadataProcessor
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.COUNT
import com.openlattice.postgres.PostgresColumn.PARTITION
import com.openlattice.postgres.PostgresMaterializedViews.Companion.PARTITION_COUNTS
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import org.springframework.stereotype.Service
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Service
class PartitionManager(hazelcastInstance: HazelcastInstance, private val hds: HikariDataSource, partitions: Int = 257) {
    private val partitionList = mutableListOf<Int>()
    private val entitySets = hazelcastInstance.getMap<UUID, EntitySet>(HazelcastMap.ENTITY_SETS.name)

    init {
        createMaterializedViewIfNotExists()
        setPartitions(partitions)
    }

    @Synchronized
    fun setPartitions(partitions: Int) {
        checkArgument(partitions > partitionList.size, "Only support increasing number of partitions.")
        //TODO: Support decreasing number of partitions, but this is unlikely to be needed, since decreasing
        //number of citus partitions will automatically have the desired effect.
        partitionList.addAll(partitionList.size until partitions)
    }

    fun setEntitySetPartitions(entitySetId: UUID, partitions: List<Int>) {
        val update = MetadataUpdate(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.of(LinkedHashSet(partitions))
        )
        entitySets.executeOnKey(entitySetId, UpdateEntitySetMetadataProcessor(update))
    }

    fun setDefaultPartitions(organizationId: UUID, partitions: List<Int>) {

    }

    fun getDefaultPartitions(organizationId: UUID): List<Int> {
        TODO("Implement this")
    }

    fun getEntitySetPartitions(entitySetId: UUID): Array<Int> {
        return entitySets.getValue(entitySetId)
    }

    fun allocatePartitions(organizationId: UUID, partitionCount: Int): List<Int> {
        checkArgument(
                partitionCount < partitionList.size,
                "Cannot request more partitions ($partitionCount) than exist (${partitionList.size}."
        )

        return getEmptiestPartitions(partitionCount).map { it.first }
    }


    fun repartition(organizationId: UUID) {
        //TODO
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

    fun getPartitionInformation(): Map<Int, Long> {
        return BasePostgresIterable(
                PreparedStatementHolderSupplier(hds, ALL_PARTITIONS) { ps ->
                    ps.setArray(1, PostgresArrays.createIntArray(ps.connection, partitionList))
                }) { it.getInt(PARTITION.name) to it.getLong(COUNT) }.toMap()
    }

    private fun getEmptiestPartitions(desiredPartitions: Int): BasePostgresIterable<Pair<Int, Long>> {
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
