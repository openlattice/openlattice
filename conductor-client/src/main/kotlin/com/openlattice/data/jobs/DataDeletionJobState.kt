package com.openlattice.data.jobs

import com.geekbeast.rhizome.jobs.JobState
import com.openlattice.data.DeleteType
import java.util.*

data class DataDeletionJobState(
        val entitySetId: UUID,
        val deleteType: DeleteType,
        val entityKeyIds: Set<UUID>? = null,
        internal var partitions: Collection<Int> = listOf(),
        internal var totalToDelete: Long = 0,
        var numDeletes: Long = 0
) : JobState