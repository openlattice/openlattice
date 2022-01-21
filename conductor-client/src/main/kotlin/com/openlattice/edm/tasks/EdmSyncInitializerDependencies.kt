package com.openlattice.edm.tasks

import com.hazelcast.core.HazelcastInstance
import com.openlattice.datastore.services.EdmManager
import com.geekbeast.tasks.HazelcastTaskDependencies

data class EdmSyncInitializerDependencies(
        val edmManager: EdmManager,
        val active: Boolean,
        val hazelcast: HazelcastInstance
) : HazelcastTaskDependencies