package com.openlattice.graph.partioning

import com.geekbeast.rhizome.jobs.JobState
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class RepartitioningJobState(
        val entitySetId: UUID,
        val oldPartitions: List<Int>,
        val newPartitions: Set<Int>,
        val batchSize: Long = 0,
        var currentlyMigratingPartitionIndex: Int = 0,
        var repartitionCount: Long = 0,
        var deleteCount: Long = 0,
        internal var needsMigrationCount: Long = 0
) : JobState