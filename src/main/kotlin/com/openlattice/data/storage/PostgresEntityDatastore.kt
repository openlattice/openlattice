package com.openlattice.data.storage

import com.codahale.metrics.annotation.Timed
import com.google.common.collect.*
import com.google.common.eventbus.EventBus
import com.openlattice.assembler.events.MaterializedEntitySetDataChangeEvent
import com.openlattice.data.EntityKeyIdService
import com.openlattice.data.EntitySetData
import com.openlattice.data.WriteEvent
import com.openlattice.data.events.EntitiesDeletedEvent
import com.openlattice.data.events.EntitiesUpsertedEvent
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.PostgresEdmManager
import com.openlattice.edm.events.EntitySetDataDeletedEvent
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.type.PropertyType
import com.openlattice.linking.LinkingQueryService
import com.openlattice.linking.PostgresLinkingFeedbackService
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PostgresIterable
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.nio.ByteBuffer
import java.util.*
import java.util.stream.Stream
import javax.inject.Inject

/**
 *
 * Manages CRUD for entities and entity sets in the system.
 */
@Service
class PostgresEntityDatastore(
        private val dataQueryService: PostgresEntityDataQueryService,
        private val edmManager: EdmManager,
        private val postgresEdmManager: PostgresEdmManager
) : EntityDatastore {

    companion object {
        private val logger = LoggerFactory.getLogger(PostgresEntityDatastore::class.java)
        const val BATCH_INDEX_THRESHOLD = 256
    }

    @Inject
    private lateinit var eventBus: EventBus

    @Inject
    private lateinit var feedbackQueryService: PostgresLinkingFeedbackService

    @Inject
    private lateinit var linkingQueryService: LinkingQueryService


    @Timed
    override fun getEntitySetData(
            entitySetId: UUID,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Map<UUID, Map<UUID, Set<Any>>> {
        return dataQueryService.getEntitiesWithPropertyTypeIds(
                ImmutableMap.of(entitySetId, Optional.empty()),
                ImmutableMap.of(entitySetId, authorizedPropertyTypes)
        ).toMap()
    }

    @Timed
    override fun getEntityKeyIdsInEntitySet(entitySetId: UUID): BasePostgresIterable<UUID> {
        return dataQueryService.getEntityKeyIdsInEntitySet(entitySetId)
    }

    @Timed
    override fun createOrUpdateEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        // need to collect linking ids before writes to the entities

        val writeEvent = dataQueryService.upsertEntities(entitySetId, entities, authorizedPropertyTypes)
        signalCreatedEntities(entitySetId, entities.keys)

        return writeEvent
    }

    @Timed
    override fun integrateEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        // need to collect linking ids before writes to the entities

        val writeEvent = dataQueryService.upsertEntities(entitySetId, entities, authorizedPropertyTypes)
        signalCreatedEntities(entitySetId, entities.keys)

        return writeEvent
    }

    @Timed
    override fun replaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        // need to collect linking ids before writes to the entities

        val writeEvent = dataQueryService.replaceEntities(entitySetId, entities, authorizedPropertyTypes)
        signalCreatedEntities(entitySetId, entities.keys)

        return writeEvent
    }

    @Timed
    override fun partialReplaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        // need to collect linking ids before writes to the entities
        val writeEvent = dataQueryService
                .partialReplaceEntities(entitySetId, entities, authorizedPropertyTypes)

        signalCreatedEntities(entitySetId, entities.keys)

        return writeEvent
    }

    private fun signalCreatedEntities(entitySetId: UUID, entityKeyIds: Set<UUID>) {
        if (shouldIndexDirectly(entitySetId, entityKeyIds)) {
            val entities = dataQueryService
                    .getEntitiesWithPropertyTypeIds(
                            ImmutableMap.of(entitySetId, Optional.of(entityKeyIds)),
                            ImmutableMap.of(entitySetId, edmManager.getPropertyTypesForEntitySet(entitySetId))
                    )
            eventBus.post(EntitiesUpsertedEvent(entitySetId, entities.toMap()))
        }

        markMaterializedEntitySetDirty(entitySetId) // mark entityset as unsync with data
        // mark all involved linking entitysets as unsync with data
        postgresEdmManager.getAllLinkingEntitySetIdsForEntitySet(entitySetId)
                .forEach { this.markMaterializedEntitySetDirty(it) }
    }

    private fun signalEntitySetDataDeleted(entitySetId: UUID) {
        eventBus.post(EntitySetDataDeletedEvent(entitySetId))
        markMaterializedEntitySetDirty(entitySetId) // mark entityset as unsync with data

        // mark all involved linking entitysets as unsync with data
        postgresEdmManager.getAllLinkingEntitySetIdsForEntitySet(entitySetId)
                .forEach { this.markMaterializedEntitySetDirty(it) }
    }

    private fun signalDeletedEntities(entitySetId: UUID, entityKeyIds: Set<UUID>) {
        if (shouldIndexDirectly(entitySetId, entityKeyIds)) {
            eventBus.post(EntitiesDeletedEvent(entitySetId, entityKeyIds))
        }

        markMaterializedEntitySetDirty(entitySetId) // mark entityset as unsync with data

        // mark all involved linking entitysets as unsync with data
        postgresEdmManager.getAllLinkingEntitySetIdsForEntitySet(entitySetId)
                .forEach { this.markMaterializedEntitySetDirty(it) }
    }

    private fun shouldIndexDirectly(entitySetId: UUID, entityKeyIds: Set<UUID>): Boolean {
        return entityKeyIds.size < BATCH_INDEX_THRESHOLD && !edmManager.getEntitySet(entitySetId).flags
                .contains(EntitySetFlag.AUDIT)
    }

    private fun markMaterializedEntitySetDirty(entitySetId: UUID) {
        eventBus.post(MaterializedEntitySetDataChangeEvent(entitySetId))
    }

    @Timed
    override fun replacePropertiesInEntities(
            entitySetId: UUID,
            replacementProperties: Map<UUID, Map<UUID, Set<Map<ByteBuffer, Any>>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {

        val writeEvent = dataQueryService
                .replacePropertiesInEntities(entitySetId, replacementProperties, authorizedPropertyTypes)
        signalCreatedEntities(entitySetId, replacementProperties.keys)

        return writeEvent
    }

    @Timed
    override fun clearEntitySet(
            entitySetId: UUID, authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        val writeEvent = dataQueryService.clearEntitySet(entitySetId, authorizedPropertyTypes)
        signalEntitySetDataDeleted(entitySetId)
        return writeEvent
    }

    @Timed
    override fun clearEntities(
            entitySetId: UUID, entityKeyIds: Set<UUID>, authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        val writeEvent = dataQueryService.clearEntities(entitySetId, entityKeyIds, authorizedPropertyTypes)
        signalDeletedEntities(entitySetId, entityKeyIds)
        return writeEvent
    }

    @Timed
    override fun clearEntityData(
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

        //If the query generated exceed 33.5M UUIDs good chance that it exceed Postgres's 1 GB max query buffer size

        return EntitySetData(
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
    override fun getEntitiesById(
            entitySetId: UUID,
            ids: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>
    ): Map<UUID, Map<FullQualifiedName, Set<Any>>> {
        return dataQueryService.getEntitiesWithPropertyTypeFqns(
                ImmutableMap.of(entitySetId, Optional.of(ids)),
                authorizedPropertyTypes
        )
    }

    @Timed
    override fun getLinkingEntities(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>
    ): Stream<MutableMap<FullQualifiedName, MutableSet<Any>>> {
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
    ): Stream<MutableMap<FullQualifiedName, MutableSet<Any>>> {
        //If the query generated exceed 33.5M UUIDs good chance that it exceed Postgres's 1 GB max query buffer size
        return dataQueryService.getEntitiesWithPropertyTypeFqns(
                entityKeyIds,
                authorizedPropertyTypes,
                emptyMap(),
                metadataOptions
        ).values.stream()
    }

    /**
     * Retrieves the authorized, linked property data for the given linking ids of entity sets.
     *
     * @param linkingIdsByEntitySetId map of linked(normal) entity set ids and their linking ids
     * @param authorizedPropertyTypesByEntitySetId map of authorized property types
     */
    @Timed
    override fun getLinkedEntityDataByLinkingId(
            linkingIdsByEntitySetId: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>
    ): Map<UUID, Map<UUID, Map<UUID, Set<Any>>>> {

        return getLinkedEntityDataByLinkingIdWithMetadata(
                linkingIdsByEntitySetId,
                authorizedPropertyTypesByEntitySetId,
                EnumSet.noneOf(MetadataOption::class.java)
        )
    }

    @Timed
    override fun getLinkedEntityDataByLinkingIdWithMetadata(
            linkingIdsByEntitySetId: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>
    ): Map<UUID, Map<UUID, Map<UUID, Set<Any>>>> {
        // TODO: Do this less terribly
        // map of: pair<linking_id, entity_set_id> to property_data
        val linkedEntityDataStream = dataQueryService.getEntitiesWithPropertyTypeIds(
                linkingIdsByEntitySetId,
                authorizedPropertyTypesByEntitySetId,
                metadataOptions = metadataOptions,
                linking = true
        )

        val linkedEntityData = HashMap<UUID, MutableMap<UUID, MutableMap<UUID, Set<Any>>>>()
//        linkedEntityDataStream.forEach { (first, second) ->
//            val primaryId = first.first //linking_id
//            val secondaryId = first.second //entity_set_id
//
//            linkedEntityData
//                    .getOrPut(primaryId) { mutableMapOf() }
//                    .getOrPut(secondaryId) { second.toMutableMap() }
//
//        }

        // linking_id/entity_set_id/property_type_id
        return linkedEntityData
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
    ): ListMultimap<UUID, MutableMap<FullQualifiedName, MutableSet<Any>>> {

        val keyCount = entitySetIdsToEntityKeyIds.keySet().size
        val avgValuesPerKey = if (entitySetIdsToEntityKeyIds.size() == 0) 0 else entitySetIdsToEntityKeyIds.size() / keyCount
        val entities =
                ArrayListMultimap
                        .create<UUID, MutableMap<FullQualifiedName, MutableSet<Any>>>(keyCount, avgValuesPerKey)

        Multimaps
                .asMap(entitySetIdsToEntityKeyIds)
                .entries
                .parallelStream()
                .forEach { (entitySetId, entityKeyIds) ->
                    val data = dataQueryService.getEntitiesWithPropertyTypeFqns(
                            mapOf(entitySetId to Optional.of(entityKeyIds)),
                            mapOf(entitySetId to authorizedPropertyTypesByEntitySet.getValue(entitySetId)),
                            emptyMap(),
                            EnumSet.noneOf(MetadataOption::class.java)
                    )
                    entities.putAll(entitySetId, data.values)
                }

        return entities
    }

    @Timed
    override fun getEntityKeyIdsOfLinkingIds(linkingIds: Set<UUID>): PostgresIterable<Pair<UUID, Set<UUID>>> {
        TODO("DELETE OR IMPLEMENT")
    }

    @Timed
    override fun getLinkingEntitySetIds(linkingId: UUID): PostgresIterable<UUID> {
        TODO("DELETE OR IMPLEMENT")
    }

    /**
     * Delete data of an entity set across ALL sync Ids.
     */
    @SuppressFBWarnings(
            value = ["UC_USELESS_OBJECT"],
            justification = "results Object is used to execute deletes in batches"
    )
    override fun deleteEntitySetData(entitySetId: UUID, authorizedPropertyTypes: Map<UUID, PropertyType>): WriteEvent {
        logger.info("Deleting data of entity set: {}", entitySetId)
        val (_, numUpdates) = dataQueryService.deleteEntitySetData(entitySetId, authorizedPropertyTypes)
        val writeEvent = dataQueryService.deleteEntitySet(entitySetId)

        signalEntitySetDataDeleted(entitySetId)

        // delete entities from linking feedbacks
        val deleteFeedbackCount = feedbackQueryService.deleteLinkingFeedbacks(entitySetId, Optional.empty())

        // Delete all neighboring entries from matched entities
        val deleteMatchCount = linkingQueryService.deleteEntitySetNeighborhood(entitySetId)

        logger.info(
                "Finished deleting data from entity set {}. " + "Deleted {} rows and {} property data, {} linking feedback and {} matched entries.",
                entitySetId,
                writeEvent.numUpdates,
                numUpdates,
                deleteFeedbackCount,
                deleteMatchCount
        )

        return writeEvent
    }

    override fun deleteEntities(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {

        val (_, numUpdates) = dataQueryService
                .deleteEntityData(entitySetId, entityKeyIds, authorizedPropertyTypes)
        val writeEvent = dataQueryService.deleteEntities(entitySetId, entityKeyIds)
        signalDeletedEntities(entitySetId, entityKeyIds)

        // delete entities from linking feedbacks too
        val deleteFeedbackCount = feedbackQueryService.deleteLinkingFeedbacks(entitySetId, Optional.of(entityKeyIds))

        // Delete all neighboring entries from matched entities
        val deleteMatchCount = linkingQueryService.deleteNeighborhoods(entitySetId, entityKeyIds)

        logger.info(
                "Finished deletion of entities ( {} ) from entity set {}. Deleted {} rows, {} property data, " + "{} linking feedback and {} matched entries.",
                entityKeyIds,
                entitySetId,
                writeEvent.numUpdates,
                numUpdates,
                deleteFeedbackCount,
                deleteMatchCount
        )

        return writeEvent
        TODO("DREW add linking logs")
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

}