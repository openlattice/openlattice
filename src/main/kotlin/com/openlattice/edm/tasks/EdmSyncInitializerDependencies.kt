package com.openlattice.edm.tasks

import com.openlattice.datastore.services.EdmManager
import com.openlattice.tasks.HazelcastTaskDependencies

data class EdmSyncInitializerDependencies(
        val edmManager: EdmManager,
        val active: Boolean
) : HazelcastTaskDependencies