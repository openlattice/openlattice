package com.openlattice.transporter.services

import com.google.common.eventbus.EventBus
import com.google.common.eventbus.Subscribe
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.events.ClearAllDataEvent
import com.openlattice.edm.events.EntityTypeCreatedEvent
import com.openlattice.edm.events.EntityTypeDeletedEvent
import com.openlattice.edm.events.PropertyTypesAddedToEntityTypeEvent
import com.openlattice.edm.set.EntitySetFlag
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*
import java.util.concurrent.ConcurrentHashMap

@Service
final class DataTransporterService(
        private val eventBus: EventBus,
        private val enterprise: HikariDataSource,
        private val dataModelService: EdmManager,
        private val partitionManager: PartitionManager,
        private val entitySetService: EntitySetManager,
        private val assemblerConfiguration: AssemblerConfiguration
)
{
    companion object {
        val logger = LoggerFactory.getLogger(DataTransporterService::class.java)
    }

    private val transporter = AssemblerConnectionManager.createDataSource( "transporter", assemblerConfiguration.server, assemblerConfiguration.ssl )
    private val entityTypeManagers: MutableMap<UUID, DataTransporterEntityTypeManager> = ConcurrentHashMap()

    init {
        dataModelService.entityTypes.map { et -> et.id to DataTransporterEntityTypeManager(et, dataModelService, partitionManager, entitySetService) }.toMap(entityTypeManagers)
        logger.info("Creating {} entity set tables", entityTypeManagers.size)
        entityTypeManagers.values.forEach {
            it.createTable(transporter, enterprise)
        }
        logger.info("Entity set tables created")
        eventBus.register(this)
    }

    public fun pollOnce() {
        val start = System.currentTimeMillis()
        val releventEntityTypeIds =
        entitySetService
                .getEntitySets()
                .filter {
                    !it.isLinking && !it.flags.contains(EntitySetFlag.AUDIT)
                }
                .map { it.entityTypeId }
                .toSet()

            releventEntityTypeIds
                .forEach {entityTypeId ->
                    entityTypeManagers[entityTypeId]?.updateAllEntitySets(enterprise, transporter)
                }
        val duration = System.currentTimeMillis() - start
        logger.info("Total poll duration time: {} ms", duration)
    }

    @Subscribe
    fun handleEntityTypeCreated(e: EntityTypeCreatedEvent) {
        val et = e.entityType
        val tableManager = DataTransporterEntityTypeManager(et, dataModelService, partitionManager, entitySetService)
        // this is idempotent and we don't want to call update methods on it from the task unless the table is there.
        tableManager.createTable(transporter, enterprise)
        entityTypeManagers.putIfAbsent(et.id, tableManager)
    }

    @Subscribe
    fun handleEntityTypeDeleted(e: EntityTypeDeletedEvent) {
        val manager = entityTypeManagers.remove(e.entityTypeId)
        manager?.removeTable(transporter, enterprise)
    }

//    @Subscribe
    fun handleClearAllData(e: ClearAllDataEvent) {
        TODO("truncate all tables")
    }

    @Subscribe
    fun handlePropertyTypesAddedToEntityTypeEvent(e: PropertyTypesAddedToEntityTypeEvent) {
        val entityTypeManager = entityTypeManagers[e.entityType.id]
        entityTypeManager?.addPropertyTypes(transporter, e.newPropertyTypes)
    }
}

