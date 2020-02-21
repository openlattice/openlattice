package com.openlattice.transporter.pods

import com.google.common.eventbus.EventBus
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.transporter.services.DataTransporterService
import com.openlattice.transporter.services.TransporterConfiguration
import com.openlattice.transporter.tasks.TransporterRunSyncTask
import com.zaxxer.hikari.HikariDataSource
import org.springframework.context.annotation.Bean
import javax.inject.Inject

class TransporterPods {
    @Inject
    private lateinit var assemblerConfiguration: AssemblerConfiguration
    @Inject
    private lateinit var entitySetManager: EntitySetManager
    @Inject
    private lateinit var partitionManager: PartitionManager
    @Inject
    private lateinit var dataModelService: EdmManager
    @Inject
    private lateinit var hikariDataSource: HikariDataSource
    @Inject
    private lateinit var hazelcastInstance: HazelcastInstance
    @Inject
    private lateinit var configuration: TransporterConfiguration
    @Inject
    private lateinit var eventBus: EventBus
    @Inject
    private lateinit var executor: ListeningExecutorService

    @Bean
    fun task(): TransporterRunSyncTask {
        return TransporterRunSyncTask()
    }

    @Bean
    fun transporterService(): DataTransporterService {
        return DataTransporterService(eventBus, hikariDataSource, dataModelService, partitionManager, entitySetManager, assemblerConfiguration)
    }
}