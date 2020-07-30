package com.openlattice.graph.partioning

import com.geekbeast.rhizome.jobs.JobState
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class RepartitioningJobState(
        val entitySetId: UUID,
        val partitions: List<Int>,
        val batchSize: Long,
        var currentlyMigratingPartitionIndex: Int = 0,
        var repartitionCount: Long = 0,
        var deleteCount: Long = 0
) : JobState