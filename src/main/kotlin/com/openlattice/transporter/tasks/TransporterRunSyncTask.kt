package com.openlattice.transporter.tasks

import com.openlattice.tasks.HazelcastFixedRateTask
import com.openlattice.tasks.HazelcastTaskDependencies
import com.openlattice.tasks.Task
import com.openlattice.transporter.services.TransporterService
import java.util.concurrent.TimeUnit

class TransporterRunSyncTaskDependencies(
        val service: TransporterService
): HazelcastTaskDependencies

class TransporterRunSyncTask : HazelcastFixedRateTask<TransporterRunSyncTaskDependencies> {
    val interval = 30L
    override fun getInitialDelay(): Long {
        return 0L
    }

    override fun getPeriod(): Long {
        return interval
    }

    override fun getTimeUnit(): TimeUnit {
        return TimeUnit.SECONDS
    }

    override fun runTask() {
        getDependency().service.pollOnce()
    }

    override fun getName(): String {
        return Task.TRANSPORTER_MATERIALIZE_DATA_REFRESH_TASK.name
    }

    override fun getDependenciesClass(): Class<out TransporterRunSyncTaskDependencies> {
        return TransporterRunSyncTaskDependencies::class.java
    }
}