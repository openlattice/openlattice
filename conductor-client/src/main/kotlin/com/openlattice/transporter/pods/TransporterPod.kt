package com.openlattice.transporter.pods

import com.google.common.eventbus.EventBus
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.transporter.services.TransporterService
import com.openlattice.transporter.tasks.TransporterInitializeServiceTask
import com.openlattice.transporter.tasks.TransporterRunSyncTask
import com.openlattice.transporter.tasks.TransporterRunSyncTaskDependencies
import com.openlattice.transporter.types.TransporterDatastore
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

    @Inject
    private lateinit var transporterDatastore: TransporterDatastore

    @Bean
    fun transporterService(): TransporterService {
        LoggerFactory.getLogger(TransporterPod::class.java).info("Constructing DataTransporterService")
        return TransporterService(
                eventBus,
                dataModelService,
                partitionManager,
                entitySetManager,
                executor,
                hazelcastInstance,
                transporterDatastore
        )
    }

    @Bean
    fun transporterInitializeServiceTask(): TransporterInitializeServiceTask {
        LoggerFactory.getLogger(TransporterPod::class.java).info("Constructing TransporterInitializeServiceTask")
        return TransporterInitializeServiceTask()
    }

    @Bean
    fun transporterRunSyncTaskDependencies(): TransporterRunSyncTaskDependencies {
        LoggerFactory.getLogger(TransporterPod::class.java).info("Constructing TransporterRunSyncTaskDependencies")
        return TransporterRunSyncTaskDependencies(transporterService())
    }

    @Bean
    fun transporterRunSyncTask(): TransporterRunSyncTask {
        LoggerFactory.getLogger(TransporterPod::class.java).info("Constructing TransporterRunSyncTask")
        return TransporterRunSyncTask()
    }
}