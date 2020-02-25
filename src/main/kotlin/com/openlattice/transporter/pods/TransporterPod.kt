package com.openlattice.transporter.pods

import com.google.common.eventbus.EventBus
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.transporter.services.DataTransporterService
import com.openlattice.transporter.tasks.TransporterRunSyncTask
import com.openlattice.transporter.tasks.TransporterRunSyncTaskDependencies
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.inject.Inject

@Configuration
class TransporterPod {
    @Inject
    private lateinit var entitySetManager: EntitySetManager
    @Inject
    private lateinit var partitionManager: PartitionManager
    @Inject
    private lateinit var dataModelService: EdmManager
    @Inject
    private lateinit var hazelcastInstance: HazelcastInstance
    @Inject
    private lateinit var eventBus: EventBus
    @Inject
    private lateinit var executor: ListeningExecutorService

    @Bean
    fun transporterService(): DataTransporterService {
        LoggerFactory.getLogger(TransporterPod::class.java).info("Constructing DataTransporterService")
        return DataTransporterService(eventBus, dataModelService, partitionManager, entitySetManager, executor, hazelcastInstance)
    }

    @Bean
    fun dep(transporterService: DataTransporterService): TransporterRunSyncTaskDependencies {
        LoggerFactory.getLogger(TransporterPod::class.java).info("Constructing TransporterRunSyncTaskDependencies")

        return TransporterRunSyncTaskDependencies(transporterService)
    }

    @Bean
    fun task(): TransporterRunSyncTask {
        LoggerFactory.getLogger(TransporterPod::class.java).info("Constructing TransporterRunSyncTask")
        return TransporterRunSyncTask()
    }

}