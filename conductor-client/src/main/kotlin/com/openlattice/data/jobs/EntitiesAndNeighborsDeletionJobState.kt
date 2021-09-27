package com.openlattice.data.jobs

import com.geekbeast.rhizome.jobs.HazelcastJobService
import com.geekbeast.rhizome.jobs.JobState
import com.openlattice.data.DeleteType
import com.openlattice.search.requests.EntityNeighborsFilter
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
data class EntitiesAndNeighborsDeletionJobState(
        val entitySetIdEntityKeyIds: MutableMap<UUID, Set<UUID>>,
        val entitySetId: UUID,
        val filter: EntityNeighborsFilter,
        val deleteType: DeleteType,
        val partitions: Map<UUID, Set<Int>>,
        val dataDeletionJobIds: MutableSet<UUID>
): JobState
