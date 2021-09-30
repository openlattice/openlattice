package com.openlattice.data.storage

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.annotation.Timed
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Multimaps
import com.google.common.collect.SetMultimap
import com.google.common.eventbus.EventBus
import com.openlattice.assembler.events.MaterializedEntitySetDataChangeEvent
import com.openlattice.data.*
import com.openlattice.data.events.EntitiesDeletedEvent
import com.openlattice.data.events.EntitiesUpsertedEvent
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.events.EntitySetDataDeletedEvent
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.type.PropertyType
import com.openlattice.linking.LinkingQueryService
import com.openlattice.linking.PostgresLinkingFeedbackService
import com.openlattice.postgres.streams.BasePostgresIterable
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.nio.ByteBuffer
import java.time.OffsetDateTime
import java.util.*
import java.util.stream.Stream
import kotlin.streams.asSequence

/**
 *
 * Manages CRUD for entities and entity sets in the system.
 */
@Service
class PostgresEntityDatastore(
        private val dataQueryService: PostgresEntityDataQueryService,
        private val edmManager: EdmManager,
        private val entitySetManager: EntitySetManager,
        metricRegistry: MetricRegistry,
        private val eventBus: EventBus,
        private val feedbackQueryService: PostgresLinkingFeedbackService,
        private val linkingQueryService: LinkingQueryService
) : EntityDatastore {

    companion object {
        private val logger = LoggerFactory.getLogger(PostgresEntityDatastore::class.java)
        const val BATCH_INDEX_THRESHOLD = 256
    }

    private val getEntitiesTimer = metricRegistry.timer(
            MetricRegistry.name(
                    PostgresEntityDatastore::class.java, "getEntities"
            )
    )
    private val getLinkedEntitiesTimer = metricRegistry.timer(
            MetricRegistry.name(
                    PostgresEntityDatastore::class.java, "getEntities(linked)"
            )
    )

    @Timed
    override fun createOrUpdateEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            propertyUpdateType: PropertyUpdateType
    ): WriteEvent {
        // need to collect linking ids before writes to the entities

        val writeEvent = dataQueryService.upsertEntities(
                entitySetId,
                entities,
                authorizedPropertyTypes,
                propertyUpdateType = propertyUpdateType
        )
        signalCreatedEntities(entitySetId, entities.keys)

        return writeEvent
    }

    @Timed
    override fun replaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            propertyUpdateType: PropertyUpdateType
    ): WriteEvent {
        // need to collect linking ids before writes to the entities

        val writeEvent = dataQueryService.replaceEntities(
                entitySetId,
                entities,
                authorizedPropertyTypes,
                propertyUpdateType
        )
        signalCreatedEntities(entitySetId, entities.keys)

        return writeEvent
    }

    @Timed
    override fun partialReplaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            propertyUpdateType: PropertyUpdateType
    ): WriteEvent {
        // need to collect linking ids before writes to the entities
        val writeEvent = dataQueryService
                .partialReplaceEntities(entitySetId, entities, authorizedPropertyTypes, propertyUpdateType)

        signalCreatedEntities(entitySetId, entities.keys)

        return writeEvent
    }

    private fun signalCreatedEntities(entitySetId: UUID, entityKeyIds: Set<UUID>) {
        if (shouldIndexDirectly(entitySetId, entityKeyIds)) {
            val propertyTypesToIndex = entitySetManager.getPropertyTypesForEntitySet(entitySetId)
                    .filter { it.value.datatype != EdmPrimitiveTypeKind.Binary }
            val entities = dataQueryService
                    .getEntitiesWithPropertyTypeIds(
                            ImmutableMap.of(entitySetId, Optional.of(entityKeyIds)),
                            ImmutableMap.of(entitySetId, propertyTypesToIndex),
                            mapOf(),
                            EnumSet.of(MetadataOption.LAST_WRITE)
                    )
            eventBus.post(EntitiesUpsertedEvent(entitySetId, entities.toMap()))
        }

        markMaterializedEntitySetDirty(entitySetId) // mark entityset as unsync with data
        // mark all involved linking entitysets as unsync with data
        edmManager.getAllLinkingEntitySetIdsForEntitySet(entitySetId)
                .forEach { this.markMaterializedEntitySetDirty(it) }
    }

    private fun signalEntitySetDataDeleted(entitySetId: UUID, deleteType: DeleteType) {
        eventBus.post(EntitySetDataDeletedEvent(entitySetId, deleteType))
        markMaterializedEntitySetDirty(entitySetId) // mark entityset as unsync with data

        // mark all involved linking entitysets as unsync with data
        edmManager.getAllLinkingEntitySetIdsForEntitySet(entitySetId)
                .forEach { this.markMaterializedEntitySetDirty(it) }
    }

    private fun signalDeletedEntities(entitySetId: UUID, entityKeyIds: Set<UUID>, deleteType: DeleteType) {
        if (shouldIndexDirectly(entitySetId, entityKeyIds)) {
            eventBus.post(EntitiesDeletedEvent(entitySetId, entityKeyIds, deleteType))
        }

        markMaterializedEntitySetDirty(entitySetId) // mark entityset as unsync with data

        // mark all involved linking entitysets as unsync with data
        edmManager.getAllLinkingEntitySetIdsForEntitySet(entitySetId)
                .forEach { this.markMaterializedEntitySetDirty(it) }
    }

    private fun shouldIndexDirectly(entitySetId: UUID, entityKeyIds: Set<UUID>): Boolean {
        return entityKeyIds.size < BATCH_INDEX_THRESHOLD
                && entitySetManager.getEntitySetIdsWithFlags(setOf(entitySetId), setOf(EntitySetFlag.AUDIT)).isEmpty()
    }

    private fun markMaterializedEntitySetDirty(entitySetId: UUID) {
        eventBus.post(MaterializedEntitySetDataChangeEvent(entitySetId))
    }

    @Timed
    override fun replacePropertiesInEntities(
            entitySetId: UUID,
            replacementProperties: Map<UUID, Map<UUID, Set<Map<ByteBuffer, Any>>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            propertyUpdateType: PropertyUpdateType
    ): WriteEvent {

        val writeEvent = dataQueryService
                .replacePropertiesInEntities(
                        entitySetId,
                        replacementProperties,
                        authorizedPropertyTypes,
                        propertyUpdateType
                )
        signalCreatedEntities(entitySetId, replacementProperties.keys)

        return writeEvent
    }

    @Timed
    override fun clearEntityProperties(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        val writeEvent = dataQueryService.clearEntityData(entitySetId, entityKeyIds, authorizedPropertyTypes)
        // same as if we updated the entities
        signalCreatedEntities(entitySetId, entityKeyIds)

        return writeEvent
    }


    @Timed
    override fun getEntities(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            orderedPropertyTypes: LinkedHashSet<String>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            linking: Boolean
    ): EntitySetData<FullQualifiedName> {
        val context = if (linking) {
            getLinkedEntitiesTimer.time()
        } else {
            getEntitiesTimer.time()
        }
        //If the query generated exceed 33.5M UUIDs good chance that it exceed Postgres's 1 GB max query buffer size

        val entitySetData = EntitySetData(
                orderedPropertyTypes,
                dataQueryService.getEntitiesWithPropertyTypeFqns(
                        entityKeyIds,
                        authorizedPropertyTypes,
                        emptyMap(),
                        EnumSet.noneOf(MetadataOption::class.java),
                        Optional.empty(),
                        linking
                ).values.asIterable()

        )

        context.stop()

        return entitySetData
    }

    @Timed
    override fun getFilteredEntitySetData(
            entitySetId: UUID,
            filteredDataPageDefinition: FilteredDataPageDefinition,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>
    ): List<Map<FullQualifiedName, Set<Any>>> {

        return dataQueryService.getEntitiesWithPropertyTypeFqns(
                entityKeyIds = ImmutableMap.of(entitySetId, Optional.empty()),
                authorizedPropertyTypes = authorizedPropertyTypes,
                filteredDataPageDefinition = filteredDataPageDefinition
        ).values.toList()
    }

    @Timed
    override fun getEntities(
            entitySetId: UUID,
            ids: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>
    ): Stream<MutableMap<FullQualifiedName, MutableSet<Any>>> {
        //If the query generated exceeds 33.5M UUIDs good chance that it exceeds Postgres's 1 GB max query buffer size
        return getEntitiesWithMetadata(
                entitySetId,
                ids,
                authorizedPropertyTypes,
                EnumSet.noneOf(MetadataOption::class.java)
        )
    }

    @Timed
    override fun getEntitiesWithMetadata(
            entitySetId: UUID,
            ids: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>
    ): Stream<MutableMap<FullQualifiedName, MutableSet<Any>>> {
        //If the query generated exceeds 33.5M UUIDs good chance that it exceeds Postgres's 1 GB max query buffer size

        return dataQueryService.getEntitiesWithPropertyTypeFqns(
                ImmutableMap.of(entitySetId, Optional.of(ids)),
                authorizedPropertyTypes,
                emptyMap(),
                metadataOptions
        ).values.stream()
    }

    @Timed
    override fun getLinkingEntities(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>
    ): Collection<MutableMap<FullQualifiedName, MutableSet<Any>>> {
        //If the query generated exceed 33.5M UUIDs good chance that it exceed Postgres's 1 GB max query buffer size
        return getLinkingEntitiesWithMetadata(
                entityKeyIds,
                authorizedPropertyTypes,
                EnumSet.noneOf(MetadataOption::class.java)
        )
    }

    @Timed
    override fun getLinkingEntitiesWithMetadata(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>
    ): Collection<MutableMap<FullQualifiedName, MutableSet<Any>>> {
        //If the query generated exceed 33.5M UUIDs good chance that it exceed Postgres's 1 GB max query buffer size
        return dataQueryService.getEntitiesWithPropertyTypeFqns(
                entityKeyIds,
                authorizedPropertyTypes,
                emptyMap(),
                metadataOptions,
                Optional.empty(),
                true
        ).values
    }

    /**
     * Retrieves the authorized, property data mapped by entity key ids as the origins of the data for each entity set
     * for the given linking ids.
     *
     * @param linkingIdsByEntitySetId map of linked(normal) entity set ids and their linking ids
     * @param authorizedPropertyTypesByEntitySetId map of authorized property types
     * @param extraMetadataOptions set of [MetadataOption]s to include in result (besides the origin id)
     */
    @Timed
    override fun getLinkedEntityDataByLinkingIdWithMetadata(
            linkingIdsByEntitySetId: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>,
            extraMetadataOptions: EnumSet<MetadataOption>
    ): Map<UUID, Map<UUID, Map<UUID, Map<UUID, Set<Any>>>>> {
        // pair<linking_id to pair<entity_set_id to pair<origin_id to property_data>>>
        val linkedEntityDataStream = dataQueryService.getLinkedEntitiesByEntitySetIdWithOriginIds(
                linkingIdsByEntitySetId,
                authorizedPropertyTypesByEntitySetId,
                extraMetadataOptions
        )

        // linking_id/entity_set_id/origin_id/property_type_id
        val linkedDataMap = HashMap<UUID, MutableMap<UUID, Map<UUID, MutableMap<UUID, MutableSet<Any>>>>>()
        linkedEntityDataStream.forEach {
            val linkingId = it.first
            val entitySetId = it.second.first
            val entityDataById = it.second.second

            if (linkedDataMap.containsKey(linkingId)) {
                linkedDataMap.getValue(linkingId)[entitySetId] = entityDataById
            } else {
                linkedDataMap[linkingId] = mutableMapOf(entitySetId to entityDataById)
            }
        }

        return linkedDataMap
    }

    @Timed
    override fun getLinkedEntitySetBreakDown(
            linkingIdsByEntitySetId: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>
    )
            : Map<UUID, Map<UUID, Map<UUID, Map<FullQualifiedName, Set<Any>>>>> {
        // pair<linking_id to pair<entity_set_id to pair<origin_id to property_data>>>
        val linkedEntityDataStream = dataQueryService.getLinkedEntitySetBreakDown(
                linkingIdsByEntitySetId,
                authorizedPropertyTypesByEntitySetId
        )

        // linking_id/entity_set_id/origin_id/property_type_id
        val linkedDataMap = HashMap<UUID, MutableMap<UUID, Map<UUID, MutableMap<FullQualifiedName, MutableSet<Any>>>>>()
        linkedEntityDataStream.forEach {
            val linkingId = it.first
            val entitySetId = it.second.first
            val entityDataById = it.second.second

            if (linkedDataMap.containsKey(linkingId)) {
                linkedDataMap.getValue(linkingId)[entitySetId] = entityDataById
            } else {
                linkedDataMap[linkingId] = mutableMapOf(entitySetId to entityDataById)
            }
        }

        return linkedDataMap
    }


    //TODO: Can be made more efficient if we are getting across same type.
    /**
     * Loads data from multiple entity sets. Note: not implemented for linking entity sets!
     *
     * @param entitySetIdsToEntityKeyIds map of entity sets to entity keys for which the data should be loaded
     * @param authorizedPropertyTypesByEntitySet map of entity sets and the property types for which the user is authorized
     * @return map of entity set ids to list of entity data
     */
    @Timed
    override fun getEntitiesAcrossEntitySets(
            entitySetIdsToEntityKeyIds: SetMultimap<UUID, UUID>,
            authorizedPropertyTypesByEntitySet: Map<UUID, Map<UUID, PropertyType>>
    ): Map<UUID, Collection<MutableMap<FullQualifiedName, MutableSet<Any>>>> {
        return Multimaps
                .asMap(entitySetIdsToEntityKeyIds)
                .entries
                .parallelStream()
                .map { (entitySetId, entityKeyIds) ->
                    val data = dataQueryService.getEntitiesWithPropertyTypeFqns(
                            mapOf(entitySetId to Optional.of(entityKeyIds)),
                            mapOf(entitySetId to authorizedPropertyTypesByEntitySet.getValue(entitySetId)),
                            emptyMap(),
                            EnumSet.noneOf(MetadataOption::class.java)
                    )
                    entitySetId to data.values
                }.asSequence().toMap()
    }

    @Timed
    override fun getEntityKeyIdsOfLinkingIds(
            linkingIds: Set<UUID>,
            normalEntitySetIds: Set<UUID>
    ): BasePostgresIterable<Pair<UUID, Set<UUID>>> {
        return linkingQueryService.getEntityKeyIdsOfLinkingIds(linkingIds, normalEntitySetIds)
    }

    override fun deleteEntityProperties(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        val propertyWriteEvent = dataQueryService
                .deleteEntityData(entitySetId, entityKeyIds, authorizedPropertyTypes)

        // same as if we updated the entities
        signalCreatedEntities(entitySetId, entityKeyIds)

        logger.info(
                "Finished deletion of properties ( {} ) from entity set {} and ( {} ) entities. Deleted {} rows " + "of property data",
                authorizedPropertyTypes.values.map(PropertyType::getType),
                entitySetId, entityKeyIds, propertyWriteEvent.numUpdates
        )

        return propertyWriteEvent
    }

    override fun getExpiringEntitiesFromEntitySet(
            entitySetId: UUID,
            expirationPolicy: DataExpiration,
            currentDateTime: OffsetDateTime
    ): BasePostgresIterable<UUID> {
        if (expirationPolicy.startDateProperty.isPresent) {
            return dataQueryService.getExpiringEntitiesFromEntitySetUsingData(
                    entitySetId,
                    expirationPolicy,
                    edmManager.getPropertyType(expirationPolicy.startDateProperty.get()),
                    currentDateTime
            )
        }

        return dataQueryService.getExpiringEntitiesFromEntitySetUsingIds(entitySetId, expirationPolicy, currentDateTime)
    }

}