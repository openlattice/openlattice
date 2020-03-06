package com.openlattice.transporter.pods

import com.google.common.eventbus.EventBus
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.kryptnostic.rhizome.configuration.RhizomeConfiguration
import com.kryptnostic.rhizome.pods.ConfigurationLoader
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.transporter.services.DataTransporterService
import com.openlattice.transporter.tasks.TransporterRunSyncTask
import com.openlattice.transporter.tasks.TransporterRunSyncTaskDependencies
import com.openlattice.transporter.types.TransporterConfiguration
import com.openlattice.transporter.types.TransporterDatastore
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.inject.Inject

@Configuration
class TransporterPod {
    @Inject
    private lateinit var rhizome: RhizomeConfiguration
    @Inject
    private lateinit var configurationLoader: ConfigurationLoader
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
    fun transporterConfiguration(): TransporterConfiguration {
        return configurationLoader.logAndLoad("transporter", TransporterConfiguration::class.java)
    }

    @Bean
    fun transporterDatastore(transporterConfiguration: TransporterConfiguration): TransporterDatastore {
        return TransporterDatastore(transporterConfiguration, rhizome)
    }

    @Bean
    fun transporterService(transporterDatastore: TransporterDatastore): DataTransporterService {
        LoggerFactory.getLogger(TransporterPod::class.java).info("Constructing DataTransporterService")
        return DataTransporterService(eventBus, dataModelService, partitionManager, entitySetManager, executor, hazelcastInstance, transporterDatastore)
    }

    @Bean
    fun transporterRunSyncTaskDependencies(transporterService: DataTransporterService): TransporterRunSyncTaskDependencies {
        LoggerFactory.getLogger(TransporterPod::class.java).info("Constructing TransporterRunSyncTaskDependencies")

        return TransporterRunSyncTaskDependencies(transporterService)
    }

    @Bean
    fun transporterRunSyncTask(): TransporterRunSyncTask {
        LoggerFactory.getLogger(TransporterPod::class.java).info("Constructing TransporterRunSyncTask")
        return TransporterRunSyncTask()
    }
}