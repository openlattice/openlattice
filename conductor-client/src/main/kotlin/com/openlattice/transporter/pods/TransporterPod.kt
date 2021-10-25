package com.openlattice.transporter.pods

import org.springframework.context.annotation.Configuration

@Configuration
class TransporterPod {

    /*
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
    */

    /*
    @Bean
    fun transporterService(): TransporterService {
        LoggerFactory.getLogger(TransporterPod::class.java).info("Constructing TransporterService")
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
    fun transporterRunSyncTask(): TransporterRunSyncTask {
        LoggerFactory.getLogger(TransporterPod::class.java).info("Constructing TransporterRunSyncTask")
        return TransporterRunSyncTask()
    }

    @Bean
    fun transporterRunSyncTaskDependencies(): TransporterRunSyncTaskDependencies {
        LoggerFactory.getLogger(TransporterPod::class.java).info("Constructing TransporterRunSyncTaskDependencies")
        return TransporterRunSyncTaskDependencies(transporterService())
    }
    */
}
