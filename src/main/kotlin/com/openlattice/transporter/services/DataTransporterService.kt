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
import com.openlattice.edm.events.*
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.type.EntityType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.transporter.processors.TransporterPropagateDataEntryProcessor
import com.openlattice.transporter.processors.TransporterSynchronizeTableDefinitionEntryProcessor
import com.openlattice.transporter.types.TransporterColumnSet
import com.zaxxer.hikari.HikariDataSource
import io.prometheus.client.Histogram
import org.slf4j.Logger
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
        val logger: Logger = LoggerFactory.getLogger(DataTransporterService::class.java)
        val pollTimer: Histogram = Histogram.build()
                .namespace("transporter")
                .name("poll_duration_seconds")
                .help("time to do one database sync poll")
                .register()
    }

    private lateinit var acm: AssemblerConnectionManager
    private lateinit var transporter: HikariDataSource

    private val transporterState = HazelcastMap.TRANSPORTER_DB_COLUMNS.getMap( hazelcastInstance )

    private fun syncTable(et: EntityType): Optional<Future<*>> {
        val prev = transporterState.putIfAbsent(et.id, TransporterColumnSet(emptyMap()))
        return if (prev == null) {
            val props = dataModelService.getPropertyTypes(et.properties)
            Optional.of(
                    transporterState.submitToKey(et.id, TransporterSynchronizeTableDefinitionEntryProcessor(props).init(acm))
            )
        } else {
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
            logger.info("synchronization finished with {} entity type tables updated", tablesCreated)
        }

        eventBus.register(this)
        return null
    }

    private fun partitions(entitySetIds: Set<UUID>): Collection<Int> {
        return partitionManager.getPartitionsByEntitySetId(entitySetIds).values.flatten().toSet()
    }

    fun pollOnce() {
        val timer = pollTimer.startTimer()
        val futures = this.transporterState.keys.map { entityTypeId ->
            val relevantEntitySets = validEntitySets(entityTypeId)
            val entitySetIds = relevantEntitySets.map { it.id }.toSet()
            val partitions = partitions(entitySetIds)
            val ft = transporterState.submitToKey(entityTypeId, TransporterPropagateDataEntryProcessor(relevantEntitySets, partitions).init(acm))
            entitySetIds.count() to ft
        }
        val setsPolled = futures.map { it.first }.sum()
        try {
            futures.forEach { it.second.get() }
        } finally {
            val duration = timer.observeDuration()
            logger.debug("Total poll duration time for {} entity sets in {} entity types: {} sec", setsPolled, futures.size, duration)
        }
    }

    private fun validEntitySets(entityTypeId: UUID): Set<EntitySet> {
        return entitySetService.getEntitySetsOfType(entityTypeId)
                .filter { !it.isLinking && !it.flags.contains(EntitySetFlag.AUDIT) }.toSet()
    }

    @Subscribe
    fun handleEntityTypeCreated(e: EntityTypeCreatedEvent) {
        this.syncTable(e.entityType)
    }

    @Subscribe
    fun handleAssociationTypeCreated(e: AssociationTypeCreatedEvent) {
        val entityType = e.associationType.associationEntityType
        if (entityType != null) {
            this.syncTable(entityType)
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
        this.transporterState.executeOnKey(e.entityType.id, TransporterSynchronizeTableDefinitionEntryProcessor(e.newPropertyTypes).init(acm))
    }
}

