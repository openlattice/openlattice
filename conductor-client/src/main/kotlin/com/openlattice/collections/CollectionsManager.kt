package com.openlattice.collections

import com.google.common.base.Preconditions.checkArgument
import com.google.common.base.Preconditions.checkState
import com.google.common.collect.Sets
import com.google.common.eventbus.EventBus
import com.hazelcast.aggregation.Aggregators
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.collections.aggregators.EntitySetCollectionConfigAggregator
import com.openlattice.collections.mapstores.ENTITY_SET_COLLECTION_ID_INDEX
import com.openlattice.collections.mapstores.ENTITY_TYPE_COLLECTION_ID_INDEX
import com.openlattice.collections.mapstores.ID_INDEX
import com.openlattice.collections.processors.AddPairToEntityTypeCollectionTemplateProcessor
import com.openlattice.collections.processors.RemoveKeyFromEntityTypeCollectionTemplateProcessor
import com.openlattice.collections.processors.UpdateEntitySetCollectionMetadataProcessor
import com.openlattice.collections.processors.UpdateEntityTypeCollectionMetadataProcessor
import com.openlattice.controllers.exceptions.ForbiddenException
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.events.EntitySetCollectionCreatedEvent
import com.openlattice.edm.events.EntitySetCollectionDeletedEvent
import com.openlattice.edm.events.EntityTypeCollectionCreatedEvent
import com.openlattice.edm.events.EntityTypeCollectionDeletedEvent
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.hazelcast.HazelcastMap
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.springframework.stereotype.Service
import java.util.*

@Service
@SuppressFBWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
class CollectionsManager(
        hazelcast: HazelcastInstance,
        private val edmManager: EdmManager,
        private val entitySetManager: EntitySetManager,
        private val aclKeyReservations: HazelcastAclKeyReservationService,
        private val schemaManager: HazelcastSchemaManager,
        private val authorizations: AuthorizationManager,
        private val eventBus: EventBus

) {

    private val entityTypeCollections = HazelcastMap.ENTITY_TYPE_COLLECTIONS.getMap(hazelcast)
    private val entitySetCollections = HazelcastMap.ENTITY_SET_COLLECTIONS.getMap(hazelcast)
    private val entitySetCollectionConfig = HazelcastMap.ENTITY_SET_COLLECTION_CONFIG.getMap(hazelcast)


    /** READ **/

    fun getAllEntityTypeCollections(): Iterable<EntityTypeCollection> {
        return entityTypeCollections.values
    }

    fun getEntityTypeCollection(id: UUID): EntityTypeCollection {
        return entityTypeCollections[id] ?: throw IllegalStateException("EntityTypeCollection $id does not exist")
    }

    fun getEntityTypeCollections(ids: Set<UUID>): Map<UUID, EntityTypeCollection> {
        return entityTypeCollections.getAll(ids)
    }

    fun getEntitySetCollection(id: UUID): EntitySetCollection {
        val entitySetCollection = entitySetCollections[id]
                ?: throw IllegalStateException("EntitySetCollection $id does not exist")

        entitySetCollection.template = getTemplatesForIds(setOf(id))[id] ?: mutableMapOf()

        return entitySetCollection
    }

    fun getEntitySetCollections(ids: Set<UUID>): Map<UUID, EntitySetCollection> {
        val templates = getTemplatesForIds(ids)
        val collections = entitySetCollections.getAll(ids)
        collections.forEach { it.value.template = templates[it.key] ?: mutableMapOf() }

        return collections
    }

    fun getEntitySetCollectionsOfType(ids: Set<UUID>, entityTypeCollectionId: UUID): Iterable<EntitySetCollection> {
        val templates = getTemplatesForIds(ids)
        val collections = entitySetCollections.values(
                Predicates.and(
                        idsPredicate(ids),
                        entityTypeCollectionIdPredicate(
                                entityTypeCollectionId
                        )
                )
        )
        collections.map { it.template = templates[it.id] ?: mutableMapOf() }

        return collections
    }

    /** CREATE **/

    fun createEntityTypeCollection(entityTypeCollection: EntityTypeCollection): UUID {

        val entityTypeIds = entityTypeCollection.template.map { it.entityTypeId }.toSet()
        val nonexistentEntityTypeIds = Sets.difference(
                entityTypeIds,
                edmManager.getEntityTypesAsMap(entityTypeIds).keys
        )

        if (nonexistentEntityTypeIds.isNotEmpty()) {
            throw IllegalStateException("EntityTypeCollection contains entityTypeIds that do not exist: $nonexistentEntityTypeIds")
        }

        aclKeyReservations.reserveIdAndValidateType(entityTypeCollection)
        val existingEntityTypeCollection = entityTypeCollections.putIfAbsent(
                entityTypeCollection.id,
                entityTypeCollection
        )

        if (existingEntityTypeCollection == null) {
            schemaManager.upsertSchemas(entityTypeCollection.schemas)
        } else {
            throw IllegalStateException("EntityTypeCollection ${entityTypeCollection.type} with id ${entityTypeCollection.id} already exists")
        }

        eventBus.post(EntityTypeCollectionCreatedEvent(entityTypeCollection))

        return entityTypeCollection.id
    }

    fun createEntitySetCollection(entitySetCollection: EntitySetCollection, autoCreate: Boolean): UUID {
        val principal = Principals.getCurrentUser()
        Principals.ensureUser(principal)

        val entityTypeCollectionId = entitySetCollection.entityTypeCollectionId

        val template = entityTypeCollections[entityTypeCollectionId]?.template ?: throw IllegalStateException(
                "Cannot create EntitySetCollection ${entitySetCollection.id} because EntityTypeCollection $entityTypeCollectionId does not exist."
        )

        if (!autoCreate) {
            checkArgument(
                    template.map { it.id }.toSet() == entitySetCollection.template.keys,
                    "EntitySetCollection ${entitySetCollection.name} template keys do not match its EntityTypeCollection template."
            )
        }

        validateEntitySetCollectionTemplate(entitySetCollection.name, entitySetCollection.template, template)

        aclKeyReservations.reserveIdAndValidateType(entitySetCollection, entitySetCollection::name)
        checkState(
                entitySetCollections.putIfAbsent(entitySetCollection.id, entitySetCollection) == null,
                "EntitySetCollection ${entitySetCollection.name} already exists."
        )

        val templateTypesToCreate = template.filter { !entitySetCollection.template.keys.contains(it.id) }
        if (templateTypesToCreate.isNotEmpty()) {
            val userPrincipal = Principals.getCurrentUser()
            val entitySetsCreated = templateTypesToCreate.associate {
                it.id to generateEntitySet(
                        entitySetCollection,
                        it,
                        userPrincipal
                )
            }
            entitySetCollection.template.putAll(entitySetsCreated)
        }

        entitySetCollectionConfig.putAll(entitySetCollection.template.entries.associate {
            CollectionTemplateKey(
                    entitySetCollection.id,
                    it.key
            ) to it.value
        })

        authorizations.setSecurableObjectType(AclKey(entitySetCollection.id), SecurableObjectType.EntitySetCollection)
        authorizations.addPermission(
                AclKey(entitySetCollection.id),
                principal,
                EnumSet.allOf(Permission::class.java)
        )

        eventBus.post(EntitySetCollectionCreatedEvent(entitySetCollection))

        return entitySetCollection.id
    }

    /** UPDATE **/

    fun updateEntityTypeCollectionMetadata(id: UUID, update: MetadataUpdate) {

        ensureEntityTypeCollectionExists(id)

        if (update.type.isPresent) {
            aclKeyReservations.renameReservation(id, update.type.get())
        }

        entityTypeCollections.submitToKey(id, UpdateEntityTypeCollectionMetadataProcessor(update))

        signalEntityTypeCollectionUpdated(id)
    }

    fun addTypeToEntityTypeCollectionTemplate(id: UUID, collectionTemplateType: CollectionTemplateType) {

        ensureEntityTypeCollectionExists(id)
        edmManager.ensureEntityTypeExists(collectionTemplateType.entityTypeId)

        val entityTypeCollection = entityTypeCollections[id]!!
        if (entityTypeCollection.template.any { it.id == collectionTemplateType.id || it.name == collectionTemplateType.name }) {
            throw IllegalArgumentException("Id or name of CollectionTemplateType $collectionTemplateType is already used on template of EntityTypeCollection $id.")
        }

        updateEntitySetCollectionsForNewType(id, collectionTemplateType)

        entityTypeCollections.executeOnKey(id, AddPairToEntityTypeCollectionTemplateProcessor(collectionTemplateType))

        signalEntityTypeCollectionUpdated(id)
    }

    private fun updateEntitySetCollectionsForNewType(
            entityTypeCollectionId: UUID, collectionTemplateType: CollectionTemplateType
    ) {

        val entitySetCollectionsToUpdate = entitySetCollections.values(
                entityTypeCollectionIdPredicate(
                        entityTypeCollectionId
                )
        ).associate { it.id to it }

        val entitySetCollectionOwners = authorizations.getOwnersForSecurableObjects(entitySetCollectionsToUpdate.keys.map {
            AclKey(
                    it
            )
        }.toSet())
        val entitySetsCreated = entitySetCollectionsToUpdate.values.associate {
            it.id to generateEntitySet(
                    it,
                    collectionTemplateType,
                    entitySetCollectionOwners.get(AclKey(it.id)).first { p -> p.type == PrincipalType.USER })
        }

        val propertyTypeIds = edmManager.getEntityType(collectionTemplateType.entityTypeId).properties
        val ownerPermissions = EnumSet.allOf(Permission::class.java)

        val permissionsToAdd = mutableMapOf<AceKey, EnumSet<Permission>>()

        entitySetsCreated.forEach {
            val entitySetCollectionId = it.key
            val entitySetId = it.value

            entitySetCollectionOwners.get(AclKey(entitySetCollectionId)).forEach { owner ->
                permissionsToAdd[AceKey(AclKey(entitySetId), owner)] = ownerPermissions
                propertyTypeIds.forEach { ptId ->
                    permissionsToAdd[AceKey(
                            AclKey(entitySetId, ptId),
                            owner
                    )] = ownerPermissions
                }
            }

            entitySetCollectionsToUpdate.getValue(entitySetCollectionId).template[collectionTemplateType.id] = entitySetId
        }

        authorizations.setPermissions(permissionsToAdd)
        entitySetCollectionConfig.putAll(entitySetCollectionsToUpdate.keys.associate {
            CollectionTemplateKey(
                    it,
                    collectionTemplateType.id
            ) to entitySetsCreated.getValue(it)
        })

        entitySetCollectionsToUpdate.values.forEach { eventBus.post(EntitySetCollectionCreatedEvent(it)) }

    }


    fun removeKeyFromEntityTypeCollectionTemplate(id: UUID, templateTypeId: UUID) {

        ensureEntityTypeCollectionExists(id)
        ensureEntityTypeCollectionNotInUse(id)

        entityTypeCollections.executeOnKey(id, RemoveKeyFromEntityTypeCollectionTemplateProcessor(templateTypeId))

        signalEntityTypeCollectionUpdated(id)
    }

    fun updateEntitySetCollectionMetadata(id: UUID, update: MetadataUpdate) {

        ensureEntitySetCollectionExists(id)

        if (update.name.isPresent) {
            aclKeyReservations.renameReservation(id, update.name.get())
        }

        if (update.organizationId.isPresent) {
            if (!authorizations.checkIfHasPermissions(
                            AclKey(update.organizationId.get()),
                            Principals.getCurrentPrincipals(),
                            EnumSet.of(Permission.READ)
                    )) {
                throw ForbiddenException("Cannot update EntitySetCollection $id with organization id ${update.organizationId.get()}")
            }
        }

        entitySetCollections.submitToKey(id, UpdateEntitySetCollectionMetadataProcessor(update))

        signalEntitySetCollectionUpdated(id)
    }

    fun updateEntitySetCollectionTemplate(id: UUID, templateUpdates: Map<UUID, UUID>) {

        val entitySetCollection = entitySetCollections[id]
                ?: throw IllegalStateException("EntitySetCollection $id does not exist")

        val entityTypeCollectionId = entitySetCollection.entityTypeCollectionId

        val template = entityTypeCollections[entityTypeCollectionId]?.template ?: throw IllegalStateException(
                "Cannot update EntitySetCollection ${entitySetCollection.id} because EntityTypeCollection $entityTypeCollectionId does not exist."
        )

        entitySetCollection.template.putAll(templateUpdates)
        validateEntitySetCollectionTemplate(entitySetCollection.name, entitySetCollection.template, template)

        entitySetCollectionConfig.putAll(templateUpdates.entries.associate {
            CollectionTemplateKey(
                    id,
                    it.key
            ) to it.value
        })

        eventBus.post(EntitySetCollectionCreatedEvent(entitySetCollection))
    }

    /** DELETE **/

    fun deleteEntityTypeCollection(id: UUID) {

        ensureEntityTypeCollectionNotInUse(id)

        entityTypeCollections.remove(id)
        aclKeyReservations.release(id)

        eventBus.post(EntityTypeCollectionDeletedEvent(id))
    }

    fun deleteEntitySetCollection(id: UUID) {
        entitySetCollections.remove(id)
        aclKeyReservations.release(id)
        entitySetCollectionConfig.removeAll(Predicates.equal(ENTITY_SET_COLLECTION_ID_INDEX, id))

        eventBus.post(EntitySetCollectionDeletedEvent(id))
    }

    /** validation **/

    fun ensureEntityTypeCollectionExists(id: UUID) {
        if (!entityTypeCollections.containsKey(id)) {
            throw IllegalStateException("EntityTypeCollection $id does not exist.")
        }
    }

    fun ensureEntitySetCollectionExists(id: UUID) {
        if (!entitySetCollections.containsKey(id)) {
            throw IllegalStateException("EntitySetCollection $id does not exist.")
        }
    }

    private fun ensureEntityTypeCollectionNotInUse(id: UUID) {
        val numEntitySetCollectionsOfType = entitySetCollections.aggregate(
                Aggregators.count(),
                entityTypeCollectionIdPredicate(id)
        )

        checkState(
                numEntitySetCollectionsOfType == 0L,
                "EntityTypeCollection $id cannot be deleted or modified because there exist $numEntitySetCollectionsOfType EntitySetCollections that use it"
        )
    }

    private fun validateEntitySetCollectionTemplate(
            name: String, mappings: Map<UUID, UUID>, template: LinkedHashSet<CollectionTemplateType>
    ) {
        val entitySets = entitySetManager.getEntitySetsAsMap(mappings.values.toSet())

        val templateEntityTypesById = template.map { it.id to it.entityTypeId }.toMap()

        mappings.forEach {
            val id = it.key
            val entitySetId = it.value
            val entitySet = entitySets[entitySetId]
                    ?: throw IllegalStateException("Could not create/update EntitySetCollection $name because entity set $entitySetId does not exist.")

            if (entitySet.entityTypeId != templateEntityTypesById[id]) {
                throw IllegalStateException(
                        "Could not create/update EntitySetCollection $name because entity set" +
                                " $entitySetId for key $id does not match template entity type ${templateEntityTypesById[id]}."
                )
            }
        }
    }

    /** predicates **/

    private fun idsPredicate(ids: Collection<UUID>): Predicate<UUID, EntitySetCollection> {
        return Predicates.`in`(ID_INDEX, *ids.toTypedArray())
    }

    private fun entityTypeCollectionIdPredicate(entityTypeCollectionId: UUID): Predicate<UUID, EntitySetCollection> {
        return Predicates.equal(ENTITY_TYPE_COLLECTION_ID_INDEX, entityTypeCollectionId)
    }

    private fun entitySetCollectionIdsPredicate(ids: Set<UUID>): Predicate<CollectionTemplateKey, UUID> {
        return Predicates.`in`(ENTITY_SET_COLLECTION_ID_INDEX, *ids.toTypedArray())
    }

    /** helpers **/

    private fun formatEntitySetName(prefix: String, templateTypeName: String): String {
        var name = prefix + "_" + templateTypeName
        name = name.toLowerCase().replace("[^a-z0-9_]".toRegex(), "")
        return getNextAvailableName(name)
    }

    private fun generateEntitySet(
            entitySetCollection: EntitySetCollection,
            collectionTemplateType: CollectionTemplateType,
            principal: Principal
    ): UUID {
        val name = formatEntitySetName(entitySetCollection.name, collectionTemplateType.name)
        val title = collectionTemplateType.title + " (" + entitySetCollection.name + ")"
        val description = "${collectionTemplateType.description}\n\nAuto-generated for EntitySetCollection ${entitySetCollection.name}"
        val flags = EnumSet.noneOf(EntitySetFlag::class.java)

        val entitySet = EntitySet(
                entityTypeId = collectionTemplateType.entityTypeId,
                name = name,
                _title = title,
                _description = description,
                contacts = mutableSetOf(),
                organizationId = entitySetCollection.organizationId,
                flags = flags
        )

        entitySetManager.createEntitySet(principal, entitySet)
        return entitySet.id
    }

    private fun getNextAvailableName(name: String): String {
        var nameAttempt = name
        var counter = 1
        while (aclKeyReservations.isReserved(nameAttempt)) {
            nameAttempt = name + "_" + counter.toString()
            counter++
        }
        return nameAttempt
    }

    private fun getTemplatesForIds(ids: Set<UUID>): MutableMap<UUID, MutableMap<UUID, UUID>> {
        return entitySetCollectionConfig.aggregate(
                EntitySetCollectionConfigAggregator(CollectionTemplates()),
                entitySetCollectionIdsPredicate(ids)
        ).templates
    }

    private fun signalEntityTypeCollectionUpdated(id: UUID) {
        eventBus.post(EntityTypeCollectionCreatedEvent(entityTypeCollections.getValue(id)))
    }

    private fun signalEntitySetCollectionUpdated(id: UUID) {
        eventBus.post(EntitySetCollectionCreatedEvent(getEntitySetCollection(id)))
    }


}