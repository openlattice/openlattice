package com.openlattice.collections

import com.google.common.base.Preconditions.checkArgument
import com.google.common.base.Preconditions.checkState
import com.google.common.collect.Sets
import com.hazelcast.aggregation.Aggregators
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
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
import com.openlattice.edm.EntitySet
import com.openlattice.edm.collection.*
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.hazelcast.HazelcastMap
import org.springframework.stereotype.Service
import java.util.*

@Service
class CollectionsManager(
        private val hazelcast: HazelcastInstance,
        private val edmManager: EdmManager,
        private val aclKeyReservations: HazelcastAclKeyReservationService,
        private val schemaManager: HazelcastSchemaManager,
        private val authorizations: AuthorizationManager

) {

    private val entityTypeCollections: IMap<UUID, EntityTypeCollection> = hazelcast.getMap(HazelcastMap.ENTITY_TYPE_COLLECTIONS.name)
    private val entitySetCollections: IMap<UUID, EntitySetCollection> = hazelcast.getMap(HazelcastMap.ENTITY_SET_COLLECTIONS.name)
    private val entitySetCollectionConfig: IMap<CollectionTemplateKey, UUID> = hazelcast.getMap(HazelcastMap.ENTITY_SET_COLLECTION_CONFIG.name)


    /** READ **/

    fun getAllEntityTypeCollections(): Iterable<EntityTypeCollection> {
        return entityTypeCollections.values
    }

    fun getEntityTypeCollection(id: UUID): EntityTypeCollection {
        return entityTypeCollections[id] ?: throw IllegalStateException("EntityTypeCollection $id does not exist")
    }

    fun getEntitySetCollection(id: UUID): EntitySetCollection {
        val entitySetCollection = entitySetCollections[id]
                ?: throw IllegalStateException("EntitySetCollection $id does not exist")

        entitySetCollection.template = getTemplatesForIds(setOf(id))[id]

        return entitySetCollection
    }

    fun getEntitySetCollections(ids: Set<UUID>): Map<UUID, EntitySetCollection> {
        val templates = getTemplatesForIds(ids)
        val collections = entitySetCollections.getAll(ids)
        collections.mapValues { it.value.template = templates[it.key].orEmpty() }

        return collections
    }

    fun getEntitySetCollectionsOfType(ids: Set<UUID>, entityTypeCollectionId: UUID): Iterable<EntitySetCollection> {
        val templates = getTemplatesForIds(ids)
        val collections = entitySetCollections.values(Predicates.and(idsPredicate(ids), entityTypeCollectionIdPredicate(entityTypeCollectionId)))
        collections.map { it.template = templates[it.id].orEmpty() }

        return collections
    }

    /** CREATE **/

    fun createEntityTypeCollection(entityTypeCollection: EntityTypeCollection): UUID {

        val entityTypeIds = entityTypeCollection.template.map { it.entityTypeId }.toSet()
        val nonexistentEntityTypeIds = Sets.difference(entityTypeIds, edmManager.getEntityTypesAsMap(entityTypeIds).keys)

        if (nonexistentEntityTypeIds.isNotEmpty()) {
            throw IllegalStateException("EntityTypeCollection contains entityTypeIds that do not exist: $nonexistentEntityTypeIds")
        }

        aclKeyReservations.reserveIdAndValidateType(entityTypeCollection)
        val existingEntityTypeCollection = entityTypeCollections.putIfAbsent(entityTypeCollection.id, entityTypeCollection)

        if (existingEntityTypeCollection == null) {
            schemaManager.upsertSchemas(entityTypeCollection.schemas)
        } else {
            throw IllegalStateException("EntityTypeCollection ${entityTypeCollection.type} with id ${entityTypeCollection.id} already exists")
        }

        return entityTypeCollection.id
    }

    fun createEntitySetCollection(entitySetCollection: EntitySetCollection): UUID {
        val principal = Principals.getCurrentUser()
        Principals.ensureUser(principal)

        var entityTypeCollectionId = entitySetCollection.entityTypeCollectionId

        val template = entityTypeCollections[entityTypeCollectionId]?.template ?: throw IllegalStateException(
                "Cannot create EntitySetCollection ${entitySetCollection.id} because EntityTypeCollection $entityTypeCollectionId does not exist.")

        checkArgument(template.map { it.id }.toSet() == entitySetCollection.template.keys,
                "EntitySetCollection ${entitySetCollection.name} template keys do not match its EntityTypeCollection template.")

        validateEntitySetCollectionTemplate(entitySetCollection.name, entitySetCollection.template, template)

        aclKeyReservations.reserveIdAndValidateType(entitySetCollection, entitySetCollection::getName)
        checkState(entitySetCollections.putIfAbsent(entitySetCollection.id, entitySetCollection) == null,
                "EntitySetCollection ${entitySetCollection.name} already exists.")

        entitySetCollectionConfig.putAll(entitySetCollection.template.entries.associate { CollectionTemplateKey(entitySetCollection.id, it.key) to it.value })

        authorizations.setSecurableObjectType(AclKey(entitySetCollection.id), SecurableObjectType.EntitySetCollection)
        authorizations.addPermission(AclKey(entitySetCollection.id),
                principal,
                EnumSet.allOf(Permission::class.java))

        return entitySetCollection.id
    }

    /** UPDATE **/

    fun updateEntityTypeCollectionMetadata(id: UUID, update: MetadataUpdate) {

        ensureEntityTypeCollectionExists(id)

        if (update.type.isPresent) {
            aclKeyReservations.renameReservation(id, update.type.get())
        }

        entityTypeCollections.submitToKey(id, UpdateEntityTypeCollectionMetadataProcessor(update))
    }

    fun addTypeToEntityTypeCollectionTemplate(id: UUID, collectionTemplateType: CollectionTemplateType) {

        ensureEntityTypeCollectionExists(id)
        ensureEntityTypeExists(collectionTemplateType.entityTypeId)

        val entityTypeCollection = entityTypeCollections[id]!!
        if (entityTypeCollection.template.any { it.id == collectionTemplateType.id || it.name == collectionTemplateType.name }) {
            throw IllegalArgumentException("Id or name of CollectionTemplateType $collectionTemplateType is already used on template of EntityTypeCollection $id.")
        }

        updateEntitySetCollectionsForNewType(id, collectionTemplateType)

        entityTypeCollections.executeOnKey(id, AddPairToEntityTypeCollectionTemplateProcessor(collectionTemplateType))
    }

    private fun updateEntitySetCollectionsForNewType(entityTypeCollectionId: UUID, collectionTemplateType: CollectionTemplateType) {

        val entitySetCollectionsToUpdate = entitySetCollections.values(entityTypeCollectionIdPredicate(entityTypeCollectionId))

        val entitySetCollectionOwners = authorizations.getOwnersForSecurableObjects(entitySetCollectionsToUpdate.map { AclKey(it.id) }.toSet())
        val entitySetsCreated = entitySetCollectionsToUpdate.associate {
            it.id to generateEntitySet(it, collectionTemplateType, entitySetCollectionOwners.get(AclKey(it.id)).first { p -> p.type == PrincipalType.USER })
        }

        val propertyTypeIds = edmManager.getEntityType(collectionTemplateType.entityTypeId).properties
        val ownerPermissions = EnumSet.allOf(Permission::class.java)

        val permissionsToAdd = mutableMapOf<AceKey, EnumSet<Permission>>()

        entitySetsCreated.forEach {
            val entitySetCollectionId = it.key
            val entitySetId = it.value

            entitySetCollectionOwners.get(AclKey(entitySetCollectionId)).forEach { owner ->
                permissionsToAdd[AceKey(AclKey(entitySetId), owner)] = ownerPermissions
                propertyTypeIds.forEach { ptId -> permissionsToAdd[AceKey(AclKey(entitySetId, ptId), owner)] = ownerPermissions }
            }
        }

        authorizations.setPermissions(permissionsToAdd)
        entitySetCollectionConfig.putAll(entitySetCollectionsToUpdate.associate { CollectionTemplateKey(it.id, collectionTemplateType.id) to entitySetsCreated.getValue(it.id) })

    }


    fun removeKeyFromEntityTypeCollectionTemplate(id: UUID, templateTypeId: UUID) {

        ensureEntityTypeCollectionExists(id)
        ensureEntityTypeCollectionNotInUse(id)

        entityTypeCollections.executeOnKey(id, RemoveKeyFromEntityTypeCollectionTemplateProcessor(templateTypeId))
    }

    fun updateEntitySetCollectionMetadata(id: UUID, update: MetadataUpdate) {

        ensureEntitySetCollectionExists(id)

        if (update.name.isPresent) {
            aclKeyReservations.renameReservation(id, update.name.get())
        }

        if (update.organizationId.isPresent) {
            if (!authorizations.checkIfHasPermissions(AclKey(update.organizationId.get()), Principals.getCurrentPrincipals(), EnumSet.of(Permission.READ))) {
                throw ForbiddenException("Cannot update EntitySetCollection $id with organization id ${update.organizationId.get()}")
            }
        }

        entitySetCollections.submitToKey(id, UpdateEntitySetCollectionMetadataProcessor(update))
    }

    fun updateEntitySetCollectionTemplate(id: UUID, templateUpdates: Map<UUID, UUID>) {

        val entitySetCollection = entitySetCollections[id]
                ?: throw IllegalStateException("EntitySetCollection $id does not exist")

        val entityTypeCollectionId = entitySetCollection.entityTypeCollectionId

        val template = entityTypeCollections[entityTypeCollectionId]?.template ?: throw IllegalStateException(
                "Cannot update EntitySetCollection ${entitySetCollection.id} because EntityTypeCollection $entityTypeCollectionId does not exist.")

        validateEntitySetCollectionTemplate(entitySetCollection.name, templateUpdates, template)

        entitySetCollectionConfig.putAll(templateUpdates.entries.associate { CollectionTemplateKey(id, it.key) to it.value })
    }

    /** DELETE **/

    fun deleteEntityTypeCollection(id: UUID) {

        ensureEntityTypeCollectionNotInUse(id)

        entityTypeCollections.remove(id)
        aclKeyReservations.release(id)
    }

    fun deleteEntitySetCollection(id: UUID) {
        entitySetCollections.remove(id)
        aclKeyReservations.release(id)
    }

    /** validation **/


    private fun ensureEntityTypeExists(id: UUID) {
        if (!edmManager.checkEntityTypeExists(id)) {
            throw IllegalArgumentException("Entity type $id does not exist")
        }
    }

    private fun ensureEntityTypeCollectionExists(id: UUID) {
        if (!entityTypeCollections.containsKey(id)) {
            throw IllegalStateException("EntityTypeCollection $id does not exist.")
        }
    }

    private fun ensureEntitySetCollectionExists(id: UUID) {
        if (!entitySetCollections.containsKey(id)) {
            throw IllegalStateException("EntitySetCollection $id does not exist.")
        }
    }

    private fun ensureEntityTypeCollectionNotInUse(id: UUID) {
        val numEntitySetCollectionsOfType = entitySetCollections.aggregate(
                Aggregators.count(),
                entityTypeCollectionIdPredicate(id) as Predicate<UUID, EntitySetCollection>)

        checkState(numEntitySetCollectionsOfType == 0L,
                "EntityTypeCollection $id cannot be deleted or modified because there exist $numEntitySetCollectionsOfType EntitySetCollections that use it")
    }

    private fun validateEntitySetCollectionTemplate(name: String, mappings: Map<UUID, UUID>, template: LinkedHashSet<CollectionTemplateType>) {
        val entitySets = edmManager.getEntitySetsAsMap(mappings.values.toSet())

        val templateEntityTypesById = template.map { it.id to it.entityTypeId }.toMap()

        mappings.forEach {
            val id = it.key
            val entitySetId = it.value
            val entitySet = entitySets[entitySetId]
                    ?: throw IllegalStateException("Could not create/update EntitySetCollection $name because entity set $entitySetId does not exist.")

            if (entitySet.entityTypeId != templateEntityTypesById[id]) {
                throw IllegalStateException("Could not create/update EntitySetCollection $name because entity set" +
                        " $entitySetId for key $id does not match template entity type ${templateEntityTypesById[id]}.")
            }
        }
    }

    /** predicates **/

    private fun idsPredicate(ids: Collection<UUID>): Predicate<*, *> {
        return Predicates.`in`(ID_INDEX, *ids.toTypedArray())
    }

    private fun entityTypeCollectionIdPredicate(entityTypeCollectionId: UUID): Predicate<*, *> {
        return Predicates.equal(ENTITY_TYPE_COLLECTION_ID_INDEX, entityTypeCollectionId)
    }

    private fun entitySetCollectionIdPredicate(id: UUID): Predicate<*, *> {
        return Predicates.equal(ENTITY_SET_COLLECTION_ID_INDEX, id)
    }

    private fun entitySetCollectionIdsPredicate(ids: Set<UUID>): Predicate<*, *> {
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

        val entitySet = EntitySet(Optional.empty(),
                collectionTemplateType.entityTypeId,
                name,
                title,
                Optional.of(description),
                setOf(),
                Optional.empty(),
                Optional.of(entitySetCollection.organizationId),
                Optional.of(flags))
        edmManager.createEntitySet(principal, entitySet)
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

    private fun getTemplatesForIds(ids: Set<UUID>): Map<UUID, Map<UUID, UUID>> {
        return entitySetCollectionConfig.aggregate(
                EntitySetCollectionConfigAggregator(CollectionTemplates()),
                entitySetCollectionIdsPredicate(ids) as Predicate<CollectionTemplateKey, UUID>).templates
    }


}