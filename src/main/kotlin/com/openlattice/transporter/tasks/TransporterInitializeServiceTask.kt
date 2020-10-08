package com.openlattice.transporter.tasks

import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.PostConstructInitializerTaskDependencies
import com.openlattice.tasks.Task

class TransporterInitializeServiceTask: HazelcastInitializationTask<TransporterRunSyncTask> {

    override fun getInitialDelay(): Long {
        return 0L
    }

    override fun getName(): String {
        return Task.TRANSPORTER_SYNC_INITIALIZATION_TASK.name
    }

    override fun getDependenciesClass(): Class<out TransporterRunSyncTask> {
        return TransporterRunSyncTask::class.java
    }

    override fun initialize(dependencies: TransporterRunSyncTask) {
        dependencies.initializeTransporterDatastore()
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf(
                PostConstructInitializerTaskDependencies.PostConstructInitializerTask::class.java
        )
    }

}
