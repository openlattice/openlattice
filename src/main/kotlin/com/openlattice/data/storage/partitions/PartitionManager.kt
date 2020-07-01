package com.openlattice.data.storage.partitions

import com.geekbeast.rhizome.hazelcast.DelegatedIntList
import com.google.common.base.Preconditions.checkArgument
import com.hazelcast.core.HazelcastInstance
import com.openlattice.data.storage.PostgresEntitySetSizesInitializationTask.Companion.ENTITY_SET_SIZES_VIEW
import com.openlattice.edm.EntitySet
import com.openlattice.edm.processors.GetPartitionsFromEntitySetEntryProcessor
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organizations.processors.OrganizationReadEntryProcessor
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.ENTITY_SETS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.StatementHolderSupplier
import com.openlattice.rhizome.DelegatedIntSet
import com.zaxxer.hikari.HikariDataSource
import org.springframework.stereotype.Service
import java.util.*


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Service
class PartitionManager @JvmOverloads constructor(
        hazelcastInstance: HazelcastInstance,
        private val hds: HikariDataSource,
        val numPartitions: Int = 257
) {
    private val DEFAULT_ORG_PARTITION_COUNT = 2
    private val partitionList = mutableListOf<Int>()
    private val entitySets = HazelcastMap.ENTITY_SETS.getMap( hazelcastInstance )
    private val organizations = HazelcastMap.ORGANIZATIONS.getMap( hazelcastInstance )

    init {
        setPartitions(numPartitions)
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
    fun allocateEntitySetPartitions(entitySet: EntitySet, partitionCount: Int = 0): EntitySet {
        isValidAllocation(partitionCount)
        val allocatedPartitions = computePartitions(entitySet, partitionCount)
        entitySet.setPartitions(allocatedPartitions)
        return entitySet
    }

    private fun computePartitions(entitySet: EntitySet, partitionCount: Int): List<Int> {
        val defaults = getDefaultPartitions(entitySet.organizationId)

        if (defaults.size >= partitionCount) {
            return defaults.shuffled().take(partitionCount)
        }

        return defaults + (partitionList - defaults).toList().shuffled().take(partitionCount - defaults.size)
    }

    fun allocateAllPartitions(entitySet: EntitySet): EntitySet {
        return allocateEntitySetPartitions(entitySet, numPartitions)
    }

    /**
     * Allocates default partitions for an organization based on emptiest partitions.
     */
    fun allocateDefaultOrganizationPartitions(organizationId: UUID, partitionCount: Int = DEFAULT_ORG_PARTITION_COUNT): List<Int> {
        return getEmptiestPartitions(partitionCount)
    }

    private fun getEmptiestPartitions(numPartitions: Int): List<Int> {
        val partitionCounts = mutableMapOf<Int, Long>()
        BasePostgresIterable(StatementHolderSupplier(hds, EMPTIEST_PARTITIONS)) {
            ResultSetAdapters.count(it) to ResultSetAdapters.partitions(it)
        }.forEach { (count, partitions) ->
            val avg = count / partitions.size
            partitions.forEach { partitionCounts[it] = partitionCounts.getOrDefault(it, 0) + avg }
        }
        return partitionCounts.entries.sortedBy { it.value }.take(numPartitions).map { it.key }
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

private val EMPTIEST_PARTITIONS = """
        SELECT $ENTITY_SET_SIZES_VIEW.$COUNT, ${ENTITY_SETS.name}.${PARTITIONS.name} 
            FROM ${ENTITY_SETS.name} 
            INNER JOIN $ENTITY_SET_SIZES_VIEW 
            ON ${ENTITY_SETS.name}.${ID.name} = $ENTITY_SET_SIZES_VIEW.${ENTITY_SET_ID.name} 
            WHERE $ENTITY_SET_SIZES_VIEW.$COUNT > 0 
            AND array_length( ${PARTITIONS.name}, 1) > 0
        """.trimIndent()

// Consistently Ordered, Unique Values, Constant Index
fun getPartition(entityKeyId: UUID, partitions: List<Int>): Int {
    return partitions[entityKeyId.leastSignificantBits.toInt() % partitions.size]
}

fun getPartition(entityKeyId: UUID, partitions: IntArray): Int {
    return partitions[entityKeyId.leastSignificantBits.toInt() % partitions.size]
}