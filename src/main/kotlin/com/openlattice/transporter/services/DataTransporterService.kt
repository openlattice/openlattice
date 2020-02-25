package com.openlattice.transporter.services

import com.google.common.eventbus.EventBus
import com.google.common.eventbus.Subscribe
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.AssemblerConnectionManagerDependent
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.events.ClearAllDataEvent
import com.openlattice.edm.events.EntityTypeCreatedEvent
import com.openlattice.edm.events.EntityTypeDeletedEvent
import com.openlattice.edm.events.PropertyTypesAddedToEntityTypeEvent
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.type.EntityType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.transporter.processors.TransporterPropagateDataEntryProcessor
import com.openlattice.transporter.processors.TransporterSynchronizeTableDefinitionEntryProcessor
import com.openlattice.transporter.types.TransporterColumnSet
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*
import java.util.concurrent.Future

@Service
final class DataTransporterService(
        private val eventBus: EventBus,
        private val dataModelService: EdmManager,
        private val partitionManager: PartitionManager,
        private val entitySetService: EntitySetManager,
        private val executor: ListeningExecutorService,
        hazelcastInstance: HazelcastInstance
): AssemblerConnectionManagerDependent<Void?>
{
    companion object {
        val logger = LoggerFactory.getLogger(DataTransporterService::class.java)
    }

    private lateinit var acm: AssemblerConnectionManager
    private lateinit var transporter: HikariDataSource

    private val transporterState = HazelcastMap.TRANSPORTER_DB_COLUMNS.getMap( hazelcastInstance )

    private fun syncTable(et: EntityType): Optional<Future<*>> {
        logger.info("syncTable({})", et.type)
        val prev = transporterState.putIfAbsent(et.id, TransporterColumnSet(emptyMap()))
        logger.info("entity type {} previously had value {}", et.type, prev)
        return if (prev == null) {
            logger.info("Synchronizing props for entity type {}", et.type)
            val props = dataModelService.getPropertyTypes(et.properties)
            Optional.of(
                    transporterState.submitToKey(et.id, TransporterSynchronizeTableDefinitionEntryProcessor(props))
            )
        } else {
            logger.info("not synchronzing props on entity type {} because previous value was {}", et.type, prev)
            Optional.empty()
        }
    }

    override fun init(acm: AssemblerConnectionManager): Void? {
        this.acm = acm
        this.transporter = acm.connect("transporter")
        executor.submit {
            val entityTypes = dataModelService.entityTypes.toList()
            logger.info("initializing DataTransporterService with {} types", entityTypes.size)
            val tablesCreated = entityTypes.map { et -> this.syncTable(et) }
                    .filter { it.isPresent }
                    .map { it.get().get() }
                    .count()
            if (tablesCreated > 0) {
                logger.info("{} entity type tables synchronized", tablesCreated)
            }
        }

        eventBus.register(this)
        return null
    }

    private fun partitions(entitySetIds: Set<UUID>): Collection<Int> {
        return partitionManager.getPartitionsByEntitySetId(entitySetIds).values.flatten().toSet()
    }

    fun pollOnce() {
        val start = System.currentTimeMillis()
        val futures = this.transporterState.keys.map { entityTypeId ->
            val relevantEntitySets = validEntitySets(entityTypeId)
            val entitySetIds = relevantEntitySets.map { it.id }.toSet()
            val partitions = partitions(entitySetIds)
            val ft = transporterState.submitToKey(entityTypeId, TransporterPropagateDataEntryProcessor(relevantEntitySets, partitions))
            entitySetIds.count() to ft
        }
        futures.forEach { it.second.get() }
        val setsPolled = futures.map { it.first }.sum()
        val duration = System.currentTimeMillis() - start
        logger.info("Total poll duration time for {} entity sets in {} entity types: {} ms", setsPolled, futures.size, duration)
    }

    private fun validEntitySets(entityTypeId: UUID): Set<EntitySet> {
        return entitySetService.getEntitySetsOfType(entityTypeId)
                .filter { !it.isLinking && !it.flags.contains(EntitySetFlag.AUDIT) }.toSet()
    }

    @Subscribe
    fun handleEntityTypeCreated(e: EntityTypeCreatedEvent) {
        if (this.transporterState.putIfAbsent(e.entityType.id, TransporterColumnSet(emptyMap())) == null) {
            val props = dataModelService.getPropertyTypes(e.entityType.properties)
            this.transporterState.submitToKey(e.entityType.id, TransporterSynchronizeTableDefinitionEntryProcessor(props))
        }
    }

    @Subscribe
    fun handleEntityTypeDeleted(e: EntityTypeDeletedEvent) {
        this.transporterState.delete(e.entityTypeId)
//        TODO("And drop the table")
    }

//    @Subscribe
    fun handleClearAllData(e: ClearAllDataEvent) {
        TODO("truncate all tables")
    }

    @Subscribe
    fun handlePropertyTypesAddedToEntityTypeEvent(e: PropertyTypesAddedToEntityTypeEvent) {
        this.transporterState.executeOnKey(e.entityType.id, TransporterSynchronizeTableDefinitionEntryProcessor(e.newPropertyTypes))
    }
}

