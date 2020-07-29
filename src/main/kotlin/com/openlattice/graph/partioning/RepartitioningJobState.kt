package com.openlattice.graph.partioning

import com.geekbeast.rhizome.jobs.JobState
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class RepartitioningJobState(
        val count: Long,
        val entitySetId: UUID,
        val batchSize: Long,
        val lastBatc
) : JobState {
}