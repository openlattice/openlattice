package com.openlattice.data.jobs

import com.geekbeast.rhizome.jobs.JobState
import com.openlattice.data.DeleteType
import java.util.*

data class DataDeletionJobState(
        val entitySetId: UUID,
        val deleteType: DeleteType,
        var partitions: Set<Int>,
        val entityKeyIds: Set<UUID>? = null,
        internal var totalToDelete: Long = 0,
        var numDeletes: Long = 0
) : JobState