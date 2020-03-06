package com.openlattice.transporter.services

import com.google.common.eventbus.EventBus
import com.google.common.eventbus.Subscribe
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.ExecutionCallback
import com.hazelcast.core.HazelcastInstance
import com.openlattice.data.events.EntitiesDeletedEvent
import com.openlattice.data.events.EntitiesUpsertedEvent
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.events.*
import com.openlattice.edm.type.EntityType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.transporter.processors.TransporterPropagateDataEntryProcessor
import com.openlattice.transporter.processors.TransporterSynchronizeTableDefinitionEntryProcessor
import com.openlattice.transporter.tableName
import com.openlattice.transporter.types.TransporterColumnSet
import com.openlattice.transporter.types.TransporterDatastore
import com.openlattice.transporter.types.transporterNamespace
import io.prometheus.client.Counter
import io.prometheus.client.Histogram
import org.eclipse.jetty.util.MultiException
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
        hazelcastInstance: HazelcastInstance,
        private val data: TransporterDatastore
)
{
    companion object {
        val logger: Logger = LoggerFactory.getLogger(DataTransporterService::class.java)
        val pollTimer: Histogram = Histogram.build()
                .namespace(transporterNamespace)
                .name("poll_duration_seconds")
                .help("time to do one database sync poll")
                .register()
        val refreshTimer: Histogram = Histogram.build()
                .namespace(transporterNamespace)
                .name("refresh_duration_seconds")
                .help("Time to do a refresh for a single entity set after a write event")
                .register()
        val errorCount: Counter = Counter.build()
                .namespace(transporterNamespace)
                .name("error_count")
                .help("Count of errors during transport operations")
                .register()
    }

    private val transporter = data.datastore()

    init {
        executor.submit {
            val entityTypes = dataModelService.entityTypes.toList()
            logger.info("initializing DataTransporterService with {} types", entityTypes.size)
            val tablesCreated = entityTypes
                    .map { et -> this.syncTable(et) }
                    .filter { it.isPresent }
                    .map { it.get().get() }
                    .count()
            logger.info("synchronization finished with {} entity type tables updated", tablesCreated)
        }
        eventBus.register(this)
    }

    private val transporterState = HazelcastMap.TRANSPORTER_DB_COLUMNS.getMap( hazelcastInstance )

    private fun syncTable(et: EntityType): Optional<Future<*>> {
        val prev = transporterState.putIfAbsent(et.id, TransporterColumnSet(emptyMap()))
        if (prev != null) {
            return Optional.empty()
        }
        val props = dataModelService.getPropertyTypes(et.properties)
        return Optional.of(
                transporterState.submitToKey(et.id, TransporterSynchronizeTableDefinitionEntryProcessor(props).init(data))
        )
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
            val ft = transporterState.submitToKey(entityTypeId, TransporterPropagateDataEntryProcessor(relevantEntitySets, partitions).init(data))
            entitySetIds.count() to ft
        }
        val setsPolled = futures.map { it.first }.sum()
        // wait for all futures to complete.
        val exception = MultiException()
        futures.forEach { (_, f) ->
            try {
                f.get()
            } catch(e: Exception) {
                errorCount.inc()
                exception.add(e)
            }
        }
        val duration = timer.observeDuration()
        logger.debug("Total poll duration time for {} entity sets in {} entity types: {} sec", setsPolled, futures.size, duration)
        exception.ifExceptionThrow()
    }

    private class TimestampExecutionCallback(val timer: Histogram.Timer) : ExecutionCallback<Any> {
        override fun onFailure(t: Throwable?) {
            timer.observeDuration()
            errorCount.inc()
        }

        override fun onResponse(response: Any?) {
            timer.observeDuration()
        }
    }

    private fun refreshDataForEntitySet(entitySetId: UUID) {
        val es = this.entitySetService.getEntitySet(entitySetId) ?: return
        val partitions = partitions(setOf(entitySetId))
        val refreshTimer = refreshTimer.startTimer()
        transporterState.submitToKey(es.entityTypeId, TransporterPropagateDataEntryProcessor(setOf(es), partitions))
                .andThen(TimestampExecutionCallback(refreshTimer))
    }

    private fun validEntitySets(entityTypeId: UUID): Set<EntitySet> {
        return entitySetService.getEntitySetsOfType(entityTypeId)
                .filter { !it.isLinking }.toSet()
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
    fun handleEntitiesUpserted(e: EntitiesUpsertedEvent) {
        this.refreshDataForEntitySet(e.entitySetId)
    }

    @Subscribe
    fun handleEntities(e: EntitiesDeletedEvent) {
        this.refreshDataForEntitySet(e.entitySetId)
    }

    @Subscribe
    fun handleEntityTypeDeleted(e: EntityTypeDeletedEvent) {
        this.transporterState.remove(e.entityTypeId)?: return
        executor.submit {
            data.datastore().connection.use { conn ->
                val st = conn.createStatement()
                st.execute("DROP TABLE ${tableName(e.entityTypeId)}")
            }
        }
    }

    @Subscribe
    fun handleClearAllData(e: ClearAllDataEvent) {
        data.datastore().connection.use {conn ->
            val st = conn.createStatement()
            this.transporterState.keys.forEach { etId ->
                st.execute("TRUNCATE TABLE ${tableName(etId)}")
            }
        }
    }

    @Subscribe
    fun handlePropertyTypesAddedToEntityTypeEvent(e: PropertyTypesAddedToEntityTypeEvent) {
        this.transporterState.executeOnKey(e.entityType.id, TransporterSynchronizeTableDefinitionEntryProcessor(e.newPropertyTypes).init(data))
    }
}

