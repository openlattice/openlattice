package com.openlattice.scheduling

import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.geekbeast.tasks.HazelcastTaskDependencies

data class ScheduledTaskServiceDependencies(
        val hazelcast: HazelcastInstance,
        val executor: ListeningExecutorService
) : HazelcastTaskDependencies