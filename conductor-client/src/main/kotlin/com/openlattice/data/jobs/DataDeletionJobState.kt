package com.openlattice.data.jobs

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.geekbeast.rhizome.jobs.JobState
import com.openlattice.data.DeleteType
import java.util.*

@JsonIgnoreProperties(value = ["partitions"])
data class DataDeletionJobState(
        val entitySetId: UUID,
        val deleteType: DeleteType,
        val entityKeyIds: MutableSet<UUID>? = null,
        internal var totalToDelete: Long = 0,
        var numDeletes: Long = 0,
        val neighborSrcEntitySetIds: Set<UUID> = setOf(),
        val neighborDstEntitySetIds: Set<UUID> = setOf()
) : JobState
