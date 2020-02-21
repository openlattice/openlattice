package com.openlattice.transporter.tasks

import com.openlattice.tasks.HazelcastFixedRateTask
import com.openlattice.tasks.HazelcastTaskDependencies
import com.openlattice.tasks.Task
import com.openlattice.transporter.services.DataTransporterService
import java.util.concurrent.TimeUnit

class TransporterRunSyncTaskDependencies(
        val service: DataTransporterService
) : HazelcastTaskDependencies

class TransporterRunSyncTask : HazelcastFixedRateTask<TransporterRunSyncTaskDependencies> {
    val delay = TimeUnit.SECONDS.toMillis(1)
    override fun getInitialDelay(): Long {
        return delay
    }

    override fun getPeriod(): Long {
        return delay
    }

    override fun getTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    override fun runTask() {
        val dep = getDependency()
        dep.service.pollOnce()
    }

    override fun getName(): String {
        return Task.TABLE_COPIER.name
    }

    override fun getDependenciesClass(): Class<out TransporterRunSyncTaskDependencies> {
        return TransporterRunSyncTaskDependencies::class.java
    }
}