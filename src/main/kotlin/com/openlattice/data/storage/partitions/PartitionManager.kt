package com.openlattice.data.storage.partitions

import com.geekbeast.rhizome.hazelcast.DelegatedIntList
import com.google.common.base.Preconditions.checkArgument
import com.hazelcast.core.HazelcastInstance
import com.openlattice.edm.EntitySet
import com.openlattice.edm.processors.GetPartitionsFromEntitySetEntryProcessor
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organizations.processors.OrganizationEntryProcessor
import com.openlattice.organizations.processors.OrganizationReadEntryProcessor
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.COUNT
import com.openlattice.postgres.PostgresColumn.PARTITION
import com.openlattice.postgres.PostgresMaterializedViews.Companion.PARTITION_COUNTS
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.openlattice.rhizome.DelegatedIntSet
import com.zaxxer.hikari.HikariDataSource
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.util.*

const val DEFAULT_PARTITION_COUNT = 2

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Service
class PartitionManager @JvmOverloads constructor(
        hazelcastInstance: HazelcastInstance,
        private val hds: HikariDataSource,
        partitions: Int = 257
) {
    private val partitionList = mutableListOf<Int>()
    private val entitySets = HazelcastMap.ENTITY_SETS.getMap( hazelcastInstance )
    private val organizations = HazelcastMap.ORGANIZATIONS.getMap( hazelcastInstance )

    init {
        createMaterializedViewIfNotExists()
        setPartitions(partitions)
    }

    fun getPartitionCount(): Int {
        return partitionList.size
    }

    @Synchronized
    fun setPartitions(partitions: Int) {
        //TODO: Support decreasing number of partitions, but this is unlikely to be needed, since decreasing
        //number of citus partitions will automatically have the desired effect.
        partitionList.addAll(partitionList.size until partitions)
    }

    fun getAllPartitions(): List<Int> {
        return partitionList
    }

    fun setDefaultPartitions(organizationId: UUID, partitions: List<Int>) {
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            it.partitions.clear()
            OrganizationEntryProcessor.Result(it.partitions.addAll(partitions))
        })
    }

    fun getDefaultPartitions(organizationId: UUID): List<Int> {
        return organizations.executeOnKey(organizationId, OrganizationReadEntryProcessor { DelegatedIntList(it.partitions) }) as DelegatedIntList
    }

    fun getEntitySetPartitions(entitySetId: UUID): Set<Int> {
        return entitySets.executeOnKey(entitySetId, GetPartitionsFromEntitySetEntryProcessor()) as DelegatedIntSet
    }

    fun getPartitionsByEntitySetId(entitySetIds: Set<UUID>): Map<UUID, Set<Int>> {
        return entitySets.executeOnKeys(entitySetIds, GetPartitionsFromEntitySetEntryProcessor()) as Map<UUID, DelegatedIntSet>
    }

    /**
     * Performs the initial allocation of partitions for an entity set based on default partitions for the organization
     * it belongs to
     * entity sets.
     *
     * @param entitySet The entity set to allocate partitions for.
     * @param partitionCount The number of partitions to attempt to assign to the entity set.
     *
     * @return Returns the entity set that was passed which has been modified with its partition allocation.
     */
    @JvmOverloads
    fun allocatePartitions(entitySet: EntitySet, partitionCount: Int = 0): EntitySet {
        isValidAllocation(partitionCount)
        val allocatedPartitions = computePartitions(entitySet, partitionCount)
        entitySet.setPartitions(allocatedPartitions)
        return entitySet
    }

    private fun computePartitions(entitySet: EntitySet, partitionCount: Int): List<Int> {
        val defaults = getDefaultPartitions(entitySet.organizationId)

        if (defaults.size >= partitionCount) {
            return defaults
        }

        return defaults + (partitionList - defaults).toList().shuffled().take(partitionCount - defaults.size)
    }

    /**
     * Allocates default partitions for an organization based on emptiest partitions.
     */
    fun allocateDefaultPartitions(organizationId: UUID, partitionCount: Int): List<Int> {
        val defaultPartitions = allocateDefaultPartitions(partitionCount)
        setDefaultPartitions(organizationId, defaultPartitions)
        return defaultPartitions
    }

    private fun createMaterializedViewIfNotExists() {
        hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(PARTITION_COUNTS.createSql)
            }
        }
    }

    @Scheduled(fixedRate = 15 * 60000) //Update every 5 minutes.
    fun refreshMaterializedView() {
        hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(PARTITION_COUNTS.refreshSql)
            }
        }
    }

    fun allocateDefaultPartitions(partitionCount: Int): List<Int> {
        return getEmptiestPartitions(partitionCount).map { it.first }
    }

    private fun getEmptiestPartitions(partitionCount: Int): BasePostgresIterable<Pair<Int, Long>> {
        //A quick note is that the partitions materialized view shouldn't be turned into a distributed table
        //with out addressing the interaction of the order by and limit clauses.
        return BasePostgresIterable(
                PreparedStatementHolderSupplier(hds, EMPTIEST_PARTITIONS) { ps ->
                    ps.setArray(1, PostgresArrays.createIntArray(ps.connection, partitionList))
                    ps.setInt(2, partitionCount)
                }) { it.getInt(PARTITION.name) to it.getLong(COUNT) }
    }

    /**
     * Checks to see if a requested allocation of partitions is valid.
     *
     * We don't have to lock here as long as number of partitions is monotonically increasing.
     */
    private fun isValidAllocation(partitionCount: Int) {
        checkArgument(
                partitionCount <= partitionList.size,
                "Cannot request more partitions ($partitionCount) than exist (${partitionList.size}."
        )
    }

}

private val ALL_PARTITIONS = "SELECT ${PARTITION.name}, COALESCE(count,0) as $COUNT FROM (SELECT unnest(?::integer[]) as ${PARTITION.name}) as partitions LEFT JOIN ${PARTITION_COUNTS.name} USING (${PARTITION.name}) ORDER BY $COUNT ASC "
private val EMPTIEST_PARTITIONS = "$ALL_PARTITIONS LIMIT ?"
