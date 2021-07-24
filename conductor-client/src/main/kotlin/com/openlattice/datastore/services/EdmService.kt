package com.openlattice.datastore.services

import com.google.common.base.Preconditions
import com.google.common.base.Preconditions.checkArgument
import com.google.common.collect.*
import com.google.common.eventbus.EventBus
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import com.openlattice.assembler.events.MaterializedEntitySetEdmChangeEvent
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.controllers.exceptions.TypeExistsException
import com.openlattice.datasets.DataSetService
import com.openlattice.datasets.SecurableObjectMetadata
import com.openlattice.edm.EntityDataModel
import com.openlattice.edm.EntityDataModelDiff
import com.openlattice.edm.EntitySet
import com.openlattice.edm.Schema
import com.openlattice.edm.events.*
import com.openlattice.edm.properties.PostgresTypeManager
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager
import com.openlattice.edm.type.*
import com.openlattice.edm.types.processors.*
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.postgres.mapstores.AssociationTypeMapstore
import com.openlattice.postgres.mapstores.EntitySetMapstore
import com.openlattice.postgres.mapstores.EntityTypeMapstore
import com.openlattice.postgres.mapstores.EntityTypePropertyMetadataMapstore
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.commons.lang3.tuple.Pair
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*
import java.util.concurrent.ConcurrentSkipListSet
import java.util.function.Consumer
import javax.inject.Inject
import kotlin.collections.LinkedHashSet

@Suppress("UnstableApiUsage")
@SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
@Service
class EdmService(
        hazelcastInstance: HazelcastInstance,
        private val aclKeyReservations: HazelcastAclKeyReservationService,
        private val authorizations: AuthorizationManager,
        private val entityTypeManager: PostgresTypeManager,
        private val schemaManager: HazelcastSchemaManager,
        private val dataSetService: DataSetService
) : EdmManager {

    private val propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap(hazelcastInstance)
    private val entityTypes = HazelcastMap.ENTITY_TYPES.getMap(hazelcastInstance)
    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)
    private val aclKeys = HazelcastMap.ACL_KEYS.getMap(hazelcastInstance)
    private val names = HazelcastMap.NAMES.getMap(hazelcastInstance)
    private val associationTypes = HazelcastMap.ASSOCIATION_TYPES.getMap(hazelcastInstance)
    private val objectMetadata = HazelcastMap.OBJECT_METADATA.getMap(hazelcastInstance)
    private val entityTypePropertyMetadata = HazelcastMap.ENTITY_TYPE_PROPERTY_METADATA.getMap(hazelcastInstance)

    init {
        propertyTypes.values.forEach { propertyType: PropertyType? -> logger.debug("Property type read: {}", propertyType) }
        entityTypes.values.forEach { entityType: EntityType? -> logger.debug("Object type read: {}", entityType) }
    }

    @Inject
    private lateinit var eventBus: EventBus

    companion object {
        private val logger = LoggerFactory.getLogger(EdmService::class.java)
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#createPropertyType(com.kryptnostic.types.PropertyType) update
     * propertyType (and return true upon success) if exists, return false otherwise
     */
    override fun createPropertyTypeIfNotExists(propertyType: PropertyType) {
        try {
            aclKeyReservations.reserveIdAndValidateType(propertyType)
        } catch (e: TypeExistsException) {
            logger.error("A type with this name already exists.")
            return
        }

        /*
         * Create property type if it doesn't exists. The reserveAclKeyAndValidateType call should ensure that
         */
        val dbRecord = propertyTypes.putIfAbsent(propertyType.id, propertyType)
        if (dbRecord == null) {
            propertyType.schemas.forEach(schemaManager.propertyTypesSchemaAdder(propertyType.id))
            eventBus.post(PropertyTypeCreatedEvent(propertyType))
        } else {
            logger.error(
                    "Inconsistency encountered in database. Verify that existing property types have all their acl keys reserved.")
        }
    }

    override fun ensureEntityTypeExists(entityTypeId: UUID) {
        Preconditions.checkState(
                entityTypes.containsKey(entityTypeId),
                "Entity type $entityTypeId does not exist."
        )
    }

    override fun ensurePropertyTypeExists(propertyTypeId: UUID) {
        Preconditions.checkState(
                propertyTypes.containsKey(propertyTypeId),
                "Property type $propertyTypeId does not exist."
        )
    }

    override fun getSrcAssocDstInvolvingEntityTypes(entityTypeIds: Set<UUID>): Triple<Set<UUID>, Set<UUID>, Set<UUID>> {
        val src = mutableSetOf<UUID>()
        val assoc = mutableSetOf<UUID>()
        val dst = mutableSetOf<UUID>()

        val entityTypeIdsArr = entityTypeIds.toTypedArray()
        associationTypes.entrySet(Predicates.or(
                Predicates.`in`<Any, Any>(AssociationTypeMapstore.ID_INDEX, *entityTypeIdsArr),
                Predicates.`in`<Any, Any>(AssociationTypeMapstore.SRC_INDEX, *entityTypeIdsArr),
                Predicates.`in`<Any, Any>(AssociationTypeMapstore.DST_INDEX, *entityTypeIdsArr)
        )).forEach { entry: Map.Entry<UUID, AssociationType> ->
            val (id, associationType) = entry

            assoc.add(id)
            val currSrc = mutableSetOf<UUID>()
            val currDst = mutableSetOf<UUID>()

            if (entityTypeIds.contains(id)) {
                currSrc.addAll(associationType.src)
                currDst.addAll(associationType.dst)
            } else {
                (associationType.src - entityTypeIds).forEach {
                    currSrc.add(it)
                    currDst.addAll(associationType.dst)
                }

                (associationType.dst - entityTypeIds).forEach {
                    currSrc.addAll(associationType.src)
                    currDst.add(it)
                }
            }
            src.addAll(currSrc)
            dst.addAll(currDst)
            if (associationType.isBidirectional) {
                src.addAll(currDst)
                dst.addAll(currSrc)
            }
        }
        return Triple(src, assoc, dst)
    }

    override fun deleteEntityType(entityTypeId: UUID) {
        /*
         * Entity types should only be deleted if there are no entity sets of that type in the system.
         */
        if (getEntitySetIdsOfType(entityTypeId).isNotEmpty()) {
            throw IllegalArgumentException(
                    "Unable to delete entity type $entityTypeId because it is associated with an entity set.")
        }

        entityTypeManager.getAssociationIdsForEntityType(entityTypeId).forEach { associationTypeId ->
            val association = getAssociationType(associationTypeId)
            if (association.src.contains(entityTypeId)) {
                removeSrcEntityTypesFromAssociationType(associationTypeId, ImmutableSet.of(entityTypeId))
            }
            if (association.dst.contains(entityTypeId)) {
                removeDstEntityTypesFromAssociationType(associationTypeId, ImmutableSet.of(entityTypeId))
            }
        }
        entityTypes.delete(entityTypeId)
        aclKeyReservations.release(entityTypeId)
        eventBus.post(EntityTypeDeletedEvent(entityTypeId))

    }

    override fun deletePropertyType(propertyTypeId: UUID) {
        val entityTypes = getEntityTypesContainPropertyType(propertyTypeId)
        if (entityTypes.all { getEntitySetIdsOfType(it.id).isEmpty() }) {
            forceDeletePropertyType(propertyTypeId)
        } else {
            throw IllegalArgumentException(
                    "Unable to delete property type because it is associated with an entity set.")
        }
    }

    override fun forceDeletePropertyType(propertyTypeId: UUID) {
        val entityTypes = getEntityTypesContainPropertyType(propertyTypeId)
        entityTypes.forEach { forceRemovePropertyTypesFromEntityType(it.id, setOf(propertyTypeId)) }
        propertyTypes.delete(propertyTypeId)
        aclKeyReservations.release(propertyTypeId)
        eventBus.post(PropertyTypeDeletedEvent(propertyTypeId))
    }

    private fun getEntityTypesContainPropertyType(propertyTypeId: UUID): Collection<EntityType> {
        return entityTypes.values(Predicates.equal(EntityTypeMapstore.PROPERTIES_INDEX, propertyTypeId))
    }

    override fun createEntityType(entityType: EntityType) {
        /*
         * This is really create or replace and should be noted as such.
         */
        aclKeyReservations.reserveIdAndValidateType(entityType)
        // Only create entity table if insert transaction succeeded.
        val existing = entityTypes.putIfAbsent(entityType.id, entityType)

        if (existing == null) {
            setUpDefaultEntityTypePropertyMetadata(entityType.id)
            /*
             * As long as schemas are registered with upsertSchema, the schema query service should pick up the schemas
             * directly from the entity types and property types tables. Longer term, we should be more explicit about
             * the magic schema registration that happens when an entity type or property type is written since the
             * services are loosely coupled in a way that makes it easy to break accidentally.
             */schemaManager.upsertSchemas(entityType.schemas)
            if (entityType.category != SecurableObjectType.AssociationType) {
                eventBus.post(EntityTypeCreatedEvent(entityType))
            }
        } else {
            /*
             * Only allow updates if entity type is not already in use.
             */
            if (getEntitySetIdsOfType(entityType.id).isEmpty()) {
                // Retrieve properties known to user
                val currentPropertyTypes = existing.properties
                // Remove the removable property types in database properly; this step takes care of removal of
                // permissions
                removePropertyTypesFromEntityType(existing.id, currentPropertyTypes - entityType.properties)
                // Add the new property types in
                addPropertyTypesToEntityType(entityType.id, entityType.properties - currentPropertyTypes)

                // Update Schema
                val currentSchemas = existing.schemas
                val entityTypeSingleton = getEntityTypeUuids(setOf(existing.type))
                (currentSchemas - entityType.schemas).forEach { schemaManager.removeEntityTypesFromSchema(entityTypeSingleton, it) }
                (entityType.schemas - currentSchemas).forEach { schemaManager.addEntityTypesToSchema(entityTypeSingleton, it) }
            }
        }
    }

    override fun getAllPropertyTypeIds(): Set<UUID> {
        return propertyTypes.keys
    }

    private fun setUpDefaultEntityTypePropertyMetadata(entityTypeId: UUID) {
        val et = getEntityType(entityTypeId)
        val propertyTags = et.propertyTags

        val espm = getPropertyTypesAsMap(et.properties).entries.associate { (propertyTypeId, property) ->
            val key = EntityTypePropertyKey(entityTypeId, propertyTypeId)
            val metadata = EntityTypePropertyMetadata(
                    property.title,
                    property.description,
                    propertyTags.getOrDefault(propertyTypeId, linkedSetOf()),
                    true
            )

            key to metadata
        }

        entityTypePropertyMetadata.putAll(espm)
    }

    override fun getAclKeyIds(aclNames: Set<String>): Map<String, UUID> {
        return aclKeys.getAll(aclNames)
    }

    override fun getEntityTypeUuids(fqns: Set<FullQualifiedName>): Set<UUID> {
        return aclKeys.getAll(fqns.map { it.fullQualifiedNameAsString }.toSet()).values.filterNotNull().toSet()
    }

    override fun getPropertyTypeId(fqn: FullQualifiedName): UUID {
        return aclKeys[fqn.fullQualifiedNameAsString]!!
    }

    override fun getEntityTypeHierarchy(entityTypeId: UUID): Set<EntityType> {
        return setOf(getEntityType(entityTypeId))
    }

    override fun getAssociationType(typeFqn: FullQualifiedName): AssociationType {
        val entityTypeId = getTypeAclKey(typeFqn)
        checkNotNull(entityTypeId) { "Entity type $typeFqn does not exist." }
        return getAssociationType(entityTypeId)
    }

    override fun getEntityType(typeFqn: FullQualifiedName): EntityType {
        val entityType = getEntityTypeSafe(typeFqn)
        checkNotNull(entityType) { "Entity type $typeFqn does not exist." }

        return entityType
    }

    override fun getEntityTypeSafe(typeFqn: FullQualifiedName): EntityType? {
        val entityTypeId = getTypeAclKey(typeFqn)
        return if (entityTypeId == null) null else getEntityType(entityTypeId)
    }

    override fun getEntityType(entityTypeId: UUID): EntityType {
        return checkNotNull(getEntityTypeSafe(entityTypeId)) {
            "Entity type of id $entityTypeId does not exist."
        }
    }

    override fun getEntityTypeSafe(entityTypeId: UUID): EntityType? {
        return entityTypes[entityTypeId]
    }

    override fun getEntityTypes(): Iterable<EntityType> {
        return entityTypeManager.getEntityTypes()
    }

    override fun getEntityTypesStrict(): Iterable<EntityType> {
        return entityTypeManager.getEntityTypesStrict()
    }

    override fun getAssociationEntityTypes(): Iterable<EntityType> {
        return entityTypeManager.getAssociationEntityTypes()
    }

    override fun getAllAssociationTypes(): Iterable<AssociationType> {
        return getAssociationTypes(associationTypes.keys).values
    }

    override fun getEntityType(namespace: String, name: String): EntityType {
        return getEntityType(FullQualifiedName(namespace, name))
    }

    override fun getPropertyType(fqn: FullQualifiedName): PropertyType {
        return checkNotNull(propertyTypes[aclKeys[fqn.toString()]]) { "Property type $fqn does not exist" }
    }

    override fun getPropertyTypesInNamespace(namespace: String): Iterable<PropertyType> {
        return entityTypeManager.getPropertyTypesInNamespace(namespace)
    }

    override fun getPropertyTypes(): Iterable<PropertyType> {
        return entityTypeManager.getPropertyTypes()
    }

    override fun addPropertyTypesToEntityType(entityTypeId: UUID, propertyTypeIds: Set<UUID>) {
        checkArgument(checkPropertyTypesExist(propertyTypeIds), "Some properties do not exist.")
        val newPropertyTypesById = propertyTypes.getAll(propertyTypeIds)
        val newPropertyTypes = newPropertyTypesById.values.toList()

        val propertyTags = getEntityType(entityTypeId).propertyTags

        val allPropertyTypes = propertyTypes.getAll(getEntityType(entityTypeId).properties).values.toList()

        val entitySets = getEntitySetsOfType(entityTypeId)
        val ownersByEntitySet = authorizations.getOwnersForSecurableObjects(entitySets.map { AclKey(it.id) })

        val aclKeysForSecurableObjectTypes = mutableSetOf<AclKey>()
        val acls = mutableListOf<Acl>()
        val newMetadata = mutableMapOf<AclKey, SecurableObjectMetadata>()

        getEntitySetsOfType(entityTypeId).forEach { entitySet ->
            val esId = entitySet.id

            val owners = ownersByEntitySet[AclKey(esId)]

            if (owners.isEmpty()) {
                return@forEach
            }

            newPropertyTypes.forEach {
                val aclKey = AclKey(esId, it.id)

                aclKeysForSecurableObjectTypes.add(aclKey)
                acls.add(Acl(aclKey, owners.map { owner -> Ace(owner, EnumSet.allOf(Permission::class.java)) }))

                val defaultMetadata = SecurableObjectMetadata.fromPropertyType(it, propertyTags.getOrDefault(it.id, linkedSetOf()))
                newMetadata[AclKey(esId, it.id)] = defaultMetadata
            }

            markMaterializedEntitySetDirtyWithEdmChanges(esId) // add edm_unsync flag for materialized views
            eventBus.post(PropertyTypesInEntitySetUpdatedEvent(esId, allPropertyTypes, false))
            eventBus.post(PropertyTypesAddedToEntitySetEvent(
                    entitySet,
                    newPropertyTypes,
                    if (entitySet.isLinking) Optional.of<Set<UUID>>(entitySet.linkedEntitySets) else Optional.empty()))
        }

        entityTypes.executeOnKey(entityTypeId, AddPropertyTypesToEntityTypeProcessor(propertyTypeIds))

        authorizations.setSecurableObjectTypes(aclKeysForSecurableObjectTypes, SecurableObjectType.PropertyTypeInEntitySet)
        authorizations.addPermissions(acls)
        objectMetadata.putAll(newMetadata)

        val entityType = getEntityType(entityTypeId)

        eventBus.post(PropertyTypesAddedToEntityTypeEvent(entityType, newPropertyTypes))
        if (entityType.category != SecurableObjectType.AssociationType) {
            eventBus.post(EntityTypeCreatedEvent(entityType))
        } else {
            eventBus.post(AssociationTypeCreatedEvent(getAssociationType(entityTypeId)))
        }

    }

    override fun removePropertyTypesFromEntityType(entityTypeId: UUID, propertyTypeIds: Set<UUID>) {
        checkArgument(checkPropertyTypesExist(propertyTypeIds), "Some properties do not exist.")

        checkArgument((getEntityType(entityTypeId).key - propertyTypeIds).isNotEmpty(),
                "Key property types cannot be removed.")
        checkArgument(getEntitySetIdsOfType(entityTypeId).isEmpty(),
                "Property types cannot be removed from entity types that have already been associated with an entity set.")
        forceRemovePropertyTypesFromEntityType(entityTypeId, propertyTypeIds)
    }

    override fun forceRemovePropertyTypesFromEntityType(entityTypeId: UUID, propertyTypeIds: Set<UUID>) {
        checkArgument(checkPropertyTypesExist(propertyTypeIds), "Some properties do not exist.")
        val entityType = getEntityType(entityTypeId)
        val id = entityType.id

        entityTypes.executeOnKey(id, RemovePropertyTypesFromEntityTypeProcessor(propertyTypeIds))

        val childEntityType = getEntityType(id)
        if (childEntityType.category != SecurableObjectType.AssociationType) {
            eventBus.post(EntityTypeCreatedEvent(childEntityType))
        } else {
            eventBus.post(AssociationTypeCreatedEvent(getAssociationType(id)))
        }
        val entitySetIdsOfEntityType = getEntitySetIdsOfType(id)

        entitySetIdsOfEntityType.forEach {
            markMaterializedEntitySetDirtyWithEdmChanges(it)
            propertyTypeIds.forEach { ptId -> dataSetService.deleteObjectMetadata(AclKey(it, ptId)) }
        }

    }

    override fun reorderPropertyTypesInEntityType(entityTypeId: UUID, propertyTypeIds: LinkedHashSet<UUID>) {
        entityTypes.executeOnKey(entityTypeId, ReorderPropertyTypesInEntityTypeProcessor(propertyTypeIds))
        val entityType = getEntityType(entityTypeId)
        if (entityType.category == SecurableObjectType.AssociationType) {
            eventBus.post(AssociationTypeCreatedEvent(getAssociationType(entityTypeId)))
        } else {
            eventBus.post(EntityTypeCreatedEvent(entityType))
        }
    }

    override fun addPrimaryKeysToEntityType(entityTypeId: UUID, propertyTypeIds: Set<UUID>) {
        checkArgument(checkPropertyTypesExist(propertyTypeIds), "Some properties do not exist.")

        var entityType = entityTypes[entityTypeId]

        checkNotNull(entityType) { "No entity type with id $entityTypeId" }
        checkArgument(entityType.properties.containsAll(propertyTypeIds),
                "Entity type $entityTypeId does not contain all the requested primary key property types.")

        entityTypes.executeOnKey(entityTypeId, AddPrimaryKeysToEntityTypeProcessor(propertyTypeIds))

        entityType = entityTypes[entityTypeId]!!
        if (entityType.category == SecurableObjectType.AssociationType) {
            eventBus.post(AssociationTypeCreatedEvent(getAssociationType(entityTypeId)))
        } else {
            eventBus.post(EntityTypeCreatedEvent(entityType))
        }
    }

    override fun removePrimaryKeysFromEntityType(entityTypeId: UUID, propertyTypeIds: Set<UUID>) {
        checkArgument(checkPropertyTypesExist(propertyTypeIds), "Some properties do not exist.")
        var entityType = entityTypes[entityTypeId]
        checkNotNull(entityType) { "No entity type with id $entityTypeId" }
        checkArgument(entityType.properties.containsAll(propertyTypeIds),
                "Entity type $entityTypeId does not contain all the requested primary key property types.")

        entityTypes.executeOnKey(entityTypeId, RemovePrimaryKeysFromEntityTypeProcessor(propertyTypeIds))

        entityType = entityTypes[entityTypeId]!!
        if (entityType.category == SecurableObjectType.AssociationType) {
            eventBus.post(AssociationTypeCreatedEvent(getAssociationType(entityTypeId)))
        } else {
            eventBus.post(EntityTypeCreatedEvent(entityType))
        }
    }

    override fun addSrcEntityTypesToAssociationType(associationTypeId: UUID, entityTypeIds: Set<UUID>) {
        checkArgument(checkEntityTypesExist(entityTypeIds))
        associationTypes.executeOnKey(associationTypeId,
                AddSrcEntityTypesToAssociationTypeProcessor(entityTypeIds))
        eventBus.post(AssociationTypeCreatedEvent(getAssociationType(associationTypeId)))
    }

    override fun addDstEntityTypesToAssociationType(associationTypeId: UUID, entityTypeIds: Set<UUID>) {
        checkArgument(checkEntityTypesExist(entityTypeIds))
        associationTypes.executeOnKey(associationTypeId,
                AddDstEntityTypesToAssociationTypeProcessor(entityTypeIds))
        eventBus.post(AssociationTypeCreatedEvent(getAssociationType(associationTypeId)))
    }

    override fun removeSrcEntityTypesFromAssociationType(associationTypeId: UUID, entityTypeIds: Set<UUID>) {
        checkArgument(checkEntityTypesExist(entityTypeIds))
        associationTypes.executeOnKey(associationTypeId,
                RemoveSrcEntityTypesFromAssociationTypeProcessor(entityTypeIds))
        eventBus.post(AssociationTypeCreatedEvent(getAssociationType(associationTypeId)))
    }

    override fun removeDstEntityTypesFromAssociationType(associationTypeId: UUID, entityTypeIds: Set<UUID>) {
        checkArgument(checkEntityTypesExist(entityTypeIds))
        associationTypes.executeOnKey(associationTypeId,
                RemoveDstEntityTypesFromAssociationTypeProcessor(entityTypeIds))
        eventBus.post(AssociationTypeCreatedEvent(getAssociationType(associationTypeId)))
    }

    override fun updatePropertyTypeMetadata(propertyTypeId: UUID, update: MetadataUpdate) {
        val propertyType = getPropertyType(propertyTypeId)
        val isFqnUpdated = update.type.isPresent
        if (isFqnUpdated) {
            aclKeyReservations.renameReservation(propertyTypeId, update.type.get())
            eventBus.post(PropertyTypeCreatedEvent(propertyType))
        }
        propertyTypes.executeOnKey(propertyTypeId, UpdatePropertyTypeMetadataProcessor(update))
        // get all entity sets containing the property type, and re-index them.
        getEntityTypesContainPropertyType(propertyTypeId).forEach {
            val properties = propertyTypes.getAll(it.properties).values.toList()

            getEntitySetIdsOfType(it.id).forEach { entitySetId: UUID ->
                if (isFqnUpdated) {
                    // add edm_unsync flag for materialized views
                    markMaterializedEntitySetDirtyWithEdmChanges(entitySetId)
                }
                eventBus.post(PropertyTypesInEntitySetUpdatedEvent(entitySetId, properties, isFqnUpdated))
            }
        }
        eventBus.post(PropertyTypeMetaDataUpdatedEvent(propertyType, update)) // currently not picked up by anything
    }

    override fun updateEntityTypeMetadata(entityTypeId: UUID, update: MetadataUpdate) {
        if (update.type.isPresent) {
            aclKeyReservations.renameReservation(entityTypeId, update.type.get())
        }
        entityTypes.executeOnKey(entityTypeId, UpdateEntityTypeMetadataProcessor(update))
        if (getEntityType(entityTypeId).category != SecurableObjectType.AssociationType) {
            eventBus.post(EntityTypeCreatedEvent(getEntityType(entityTypeId)))
        } else {
            eventBus.post(AssociationTypeCreatedEvent(getAssociationType(entityTypeId)))
        }
    }

    private fun markMaterializedEntitySetDirtyWithEdmChanges(entitySetId: UUID) {
        eventBus.post(MaterializedEntitySetEdmChangeEvent(entitySetId))
    }

    /**************
     * Validation
     */
    override fun checkPropertyTypesExist(properties: Set<UUID>): Boolean {
        return properties.all { propertyTypes.containsKey(it) }
    }

    override fun checkPropertyTypeExists(propertyTypeId: UUID): Boolean {
        return propertyTypes.containsKey(propertyTypeId)
    }

    override fun checkEntityTypesExist(entityTypeIds: Set<UUID>): Boolean {
        return entityTypeIds.all { entityTypes.containsKey(it) }
    }

    override fun getPropertyTypes(propertyIds: Set<UUID>): Collection<PropertyType> {
        return propertyTypes.getAll(propertyIds).values
    }

    override fun getTypeAclKey(type: FullQualifiedName): UUID? {
        return aclKeys[type.toString()]
    }

    override fun getPropertyType(propertyTypeId: UUID): PropertyType? {
        return propertyTypes[propertyTypeId]
    }

    override fun getPropertyTypeFqn(propertyTypeId: UUID): FullQualifiedName {
        return FullQualifiedName(names[propertyTypeId])
    }

    override fun getFqnToIdMap(propertyTypeFqns: Set<FullQualifiedName>): Map<FullQualifiedName, UUID> {
        return aclKeys.getAll(propertyTypeFqns.map { it.fullQualifiedNameAsString }.toSet()).entries.associate {
            FullQualifiedName(it.key) to it.value
        }
    }

    override fun getPropertyTypesAsMap(propertyTypeIds: Set<UUID>): Map<UUID, PropertyType> {
        return propertyTypes.getAll(propertyTypeIds)
    }

    override fun getEntityTypesAsMap(entityTypeIds: Set<UUID>): Map<UUID, EntityType> {
        return entityTypes.getAll(entityTypeIds)
    }

    override fun getPropertyTypesOfEntityType(entityTypeId: UUID): Map<UUID, PropertyType> {
        return propertyTypes.getAll(getEntityType(entityTypeId).properties)
    }

    override fun createAssociationType(associationType: AssociationType, entityTypeId: UUID): UUID {
        val existing = associationTypes.putIfAbsent(entityTypeId, associationType)
        if (existing != null) {
            logger.error(
                    "Inconsistency encountered in database. Verify that existing association types have all their acl keys reserved.")
        }
        eventBus.post(AssociationTypeCreatedEvent(associationType))
        return entityTypeId
    }

    private fun getAssociationTypes(ids: Set<UUID>): Map<UUID, AssociationType> {
        val baseAssociationTypes = associationTypes.getAll(ids)
        val aEntityTypes = entityTypes.getAll(ids)

        return baseAssociationTypes.filter { aEntityTypes.containsKey(it.key) }.mapValues {
            AssociationType(
                    Optional.of(aEntityTypes.getValue(it.key)),
                    it.value.src,
                    it.value.dst,
                    it.value.isBidirectional
            )
        }
    }

    override fun getAssociationType(associationTypeId: UUID): AssociationType {
        val associationDetails = getAssociationTypeDetails(associationTypeId)
        val entityType = Optional.ofNullable(entityTypes[associationTypeId])
        return AssociationType(
                entityType,
                associationDetails.src,
                associationDetails.dst,
                associationDetails.isBidirectional)
    }

    private fun getAssociationTypeDetails(id: UUID): AssociationType {
        return checkNotNull(associationTypes[id]) { "Association type of id $id does not exist." }
    }

    override fun getAssociationTypeSafe(associationTypeId: UUID): AssociationType? {
        val associationDetails = associationTypes[associationTypeId]
        val entityType = entityTypes[associationTypeId]
        return if (associationDetails == null || entityType == null) {
            null
        } else AssociationType(
                Optional.of(entityType),
                associationDetails.src,
                associationDetails.dst,
                associationDetails.isBidirectional)
    }

    override fun deleteAssociationType(associationTypeId: UUID) {
        val associationType = getAssociationType(associationTypeId)
        if (associationType.associationEntityType == null) {
            logger.error("Inconsistency found: association type of id {} has no associated entity type",
                    associationTypeId)
            throw IllegalStateException("Failed to delete association type of id $associationTypeId")
        }
        deleteEntityType(associationType.associationEntityType.id)
        associationTypes.delete(associationTypeId)
        eventBus.post(AssociationTypeDeletedEvent(associationTypeId))
    }

    override fun getAssociationDetails(associationTypeId: UUID): AssociationDetails {
        val associationType = getAssociationTypeDetails(associationTypeId)
        val entityTypesById = entityTypes.getAll(associationType.src + associationType.dst)

        val srcEntityTypes = LinkedHashSet(associationType.src.mapNotNull { entityTypesById[it] })
        val dstEntityTypes = LinkedHashSet(associationType.dst.mapNotNull { entityTypesById[it] })

        return AssociationDetails(srcEntityTypes, dstEntityTypes, associationType.isBidirectional)
    }

    override fun getAvailableAssociationTypesForEntityType(entityTypeId: UUID): Iterable<EntityType> {
        return entityTypeManager.getAssociationIdsForEntityType(entityTypeId).mapNotNull { entityTypes[it] }
    }

    private fun createOrUpdatePropertyTypeWithFqn(pt: PropertyType, fqn: FullQualifiedName) {
        val existing = getPropertyType(pt.id)
        if (existing == null) {
            createPropertyTypeIfNotExists(pt)
        } else {
            val optionalTitleUpdate = if (pt.title == existing.title) Optional.empty() else Optional.of(pt.title)
            val optionalDescriptionUpdate = if (pt.description == existing.description) Optional.empty() else Optional.of(pt.description)
            val optionalFqnUpdate = if (fqn == existing.type) Optional.empty() else Optional.of(fqn)
            val optionalPiiUpdate = if (pt.isPii == existing.isPii) Optional.empty() else Optional.of(pt.isPii)
            updatePropertyTypeMetadata(existing.id, MetadataUpdate(
                    optionalTitleUpdate,
                    optionalDescriptionUpdate,
                    Optional.empty(),
                    Optional.empty(),
                    optionalFqnUpdate,
                    optionalPiiUpdate,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()))
        }
    }

    private fun createOrUpdatePropertyType(pt: PropertyType?) {
        createOrUpdatePropertyTypeWithFqn(pt!!, pt.type)
    }

    private fun createOrUpdateEntityTypeWithFqn(et: EntityType, fqn: FullQualifiedName) {
        val existing = getEntityTypeSafe(et.id)
        if (existing == null) {
            createEntityType(et)
        } else {
            val optionalTitleUpdate = if (et.title == existing.title) Optional.empty() else Optional.of(et.title)
            val optionalDescriptionUpdate = if (et.description == existing.description) Optional.empty() else Optional.of(et.description)
            val optionalFqnUpdate = if (fqn == existing.type) Optional.empty() else Optional.of(fqn)
            val optionalPropertyTagsUpdate = if (et.propertyTags
                    == existing.propertyTags) Optional.empty() else Optional.of(existing.propertyTags)
            updateEntityTypeMetadata(existing.id, MetadataUpdate(
                    optionalTitleUpdate,
                    optionalDescriptionUpdate,
                    Optional.empty(),
                    Optional.empty(),
                    optionalFqnUpdate,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    optionalPropertyTagsUpdate,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()))
            if (et.properties != existing.properties) {
                addPropertyTypesToEntityType(existing.id, et.properties)
            }
        }
    }

    private fun createOrUpdateEntityType(et: EntityType?) {
        createOrUpdateEntityTypeWithFqn(et!!, et.type)
    }

    private fun createOrUpdateAssociationTypeWithFqn(at: AssociationType, fqn: FullQualifiedName) {
        val et = at.associationEntityType
        val existing = getAssociationTypeSafe(et.id)
        if (existing == null) {
            createOrUpdateEntityTypeWithFqn(et, fqn)
            createAssociationType(at, getTypeAclKey(et.type)!!)
        } else {
            if (existing.src != at.src) {
                addSrcEntityTypesToAssociationType(existing.associationEntityType.id, at.src)
            }
            if (existing.dst != at.dst) {
                addDstEntityTypesToAssociationType(existing.associationEntityType.id, at.dst)
            }
        }
    }

    private fun createOrUpdateAssociationType(at: AssociationType?) {
        createOrUpdateAssociationTypeWithFqn(at!!, at.associationEntityType.type)
    }

    private fun resolveFqnCycles(
            id: UUID?,
            objectType: SecurableObjectType?,
            propertyTypesById: Map<UUID, PropertyType>,
            entityTypesById: Map<UUID, EntityType>,
            associationTypesById: Map<UUID, AssociationType>) {
        when (objectType) {
            SecurableObjectType.PropertyTypeInEntitySet -> createOrUpdatePropertyType(propertyTypesById[id])
            SecurableObjectType.EntityType -> createOrUpdateEntityType(entityTypesById[id])
            SecurableObjectType.AssociationType -> createOrUpdateAssociationType(associationTypesById[id])
            else -> {
            }
        }
    }

    override fun getEntityDataModelDiff(edm: EntityDataModel): EntityDataModelDiff {
        return getEntityDataModelDiffAndFqnLists(edm).left
    }

    private fun getEntityDataModelDiffAndFqnLists(edm: EntityDataModel): Pair<EntityDataModelDiff, Set<List<UUID?>>> {
        val conflictingPropertyTypes = ConcurrentSkipListSet(Comparator
                .comparing { propertyType: PropertyType -> propertyType.type.toString() })
        val conflictingEntityTypes = ConcurrentSkipListSet(Comparator
                .comparing { entityType: EntityType -> entityType.type.toString() })
        val conflictingAssociationTypes = ConcurrentSkipListSet(Comparator
                .comparing { associationType: AssociationType -> associationType.associationEntityType.type.toString() })
        val updatedPropertyTypes = ConcurrentSkipListSet(Comparator
                .comparing { propertyType: PropertyType -> propertyType.type.toString() })
        val updatedEntityTypes = ConcurrentSkipListSet(Comparator
                .comparing { entityType: EntityType -> entityType.type.toString() })
        val updatedAssociationTypes = ConcurrentSkipListSet(Comparator
                .comparing { associationType: AssociationType -> associationType.associationEntityType.type.toString() })
        val updatedSchemas = ConcurrentSkipListSet(Comparator
                .comparing { schema: Schema -> schema.fqn.toString() })
        val idsToFqns: MutableMap<UUID, FullQualifiedName> = Maps.newHashMap()
        val idsToTypes: MutableMap<UUID, SecurableObjectType> = Maps.newHashMap()
        val propertyTypesById: MutableMap<UUID, PropertyType> = Maps.newHashMap()
        val entityTypesById: MutableMap<UUID, EntityType> = Maps.newHashMap()
        val associationTypesById: MutableMap<UUID, AssociationType> = Maps.newHashMap()
        edm.propertyTypes.forEach(Consumer { pt: PropertyType ->
            val existing = getPropertyType(pt.id)
            if (existing == null) {
                updatedPropertyTypes.add(pt)
            } else if (existing != pt) {
                if (pt.datatype != existing.datatype
                        || pt.analyzer != existing.analyzer) {
                    conflictingPropertyTypes.add(pt)
                } else if (pt.type != existing.type) {
                    idsToTypes[pt.id] = SecurableObjectType.PropertyTypeInEntitySet
                    idsToFqns[pt.id] = pt.type
                    propertyTypesById[pt.id] = pt
                } else if (pt.title != existing.title
                        || pt.description != existing.description
                        || !pt.isPii == existing.isPii) {
                    updatedPropertyTypes.add(pt)
                }
            }
        })
        edm.entityTypes.forEach(Consumer { et: EntityType ->
            val existing = getEntityTypeSafe(et.id)
            if (existing == null) {
                updatedEntityTypes.add(et)
            } else if (existing != et) {
                if (et.baseType != existing.baseType
                        || et.category != existing.category
                        || et.key != existing.key) {
                    conflictingEntityTypes.add(et)
                } else if (et.type != existing.type) {
                    idsToTypes[et.id] = SecurableObjectType.EntityType
                    idsToFqns[et.id] = et.type
                    entityTypesById[et.id] = et
                } else if (et.title != existing.title
                        || et.description != existing.description
                        || et.properties != existing.properties) {
                    updatedEntityTypes.add(et)
                }
            }
        })
        edm.associationTypes.forEach(Consumer { at: AssociationType ->
            val atEntityType = at.associationEntityType
            val existing = getAssociationTypeSafe(atEntityType.id)
            if (existing == null) {
                updatedAssociationTypes.add(at)
            } else if (existing != at) {
                if (!at.isBidirectional == existing.isBidirectional || atEntityType.baseType != existing.associationEntityType.baseType
                        || atEntityType.category != existing.associationEntityType.category
                        || atEntityType.key != existing.associationEntityType.key) {
                    conflictingAssociationTypes.add(at)
                } else if (atEntityType.type != existing.associationEntityType.type) {
                    idsToTypes[atEntityType.id] = SecurableObjectType.AssociationType
                    idsToFqns[atEntityType.id] = atEntityType.type
                    associationTypesById[atEntityType.id] = at
                } else if (atEntityType.title != existing.associationEntityType.title
                        || atEntityType.description != existing.associationEntityType.description
                        || atEntityType.properties != existing.associationEntityType.properties
                        || at.src != existing.src
                        || at.dst != existing.dst) {
                    updatedAssociationTypes.add(at)
                }
            }
        })
        edm.schemas.forEach(Consumer { schema: Schema ->
            var existing: Schema? = null
            if (schemaManager.checkSchemaExists(schema.fqn)) {
                existing = schemaManager.getSchema(schema.fqn.namespace, schema.fqn.name)
            }
            if (schema != existing) {
                updatedSchemas.add(schema)
            }
        })
        val cyclesAndConflicts = checkFqnDiffs(idsToFqns)
        val idsToOutcome: MutableMap<UUID?, Boolean> = Maps.newHashMap()
        cyclesAndConflicts[0].forEach(Consumer { idList: List<UUID?> -> idList.forEach(Consumer { id: UUID? -> idsToOutcome[id] = true }) })
        cyclesAndConflicts[1].forEach(Consumer { idList: List<UUID?> -> idList.forEach(Consumer { id: UUID? -> idsToOutcome[id] = false }) })
        idsToOutcome.forEach { (id: UUID?, shouldResolve: Boolean) ->
            when (idsToTypes[id]) {
                SecurableObjectType.PropertyTypeInEntitySet -> if (shouldResolve) {
                    updatedPropertyTypes.add(propertyTypesById[id])
                } else {
                    conflictingPropertyTypes.add(propertyTypesById[id])
                }
                SecurableObjectType.EntityType -> if (shouldResolve) {
                    updatedEntityTypes.add(entityTypesById[id])
                } else {
                    conflictingEntityTypes.add(entityTypesById[id])
                }
                SecurableObjectType.AssociationType -> if (shouldResolve) {
                    updatedAssociationTypes.add(associationTypesById[id])
                } else {
                    conflictingAssociationTypes.add(associationTypesById[id])
                }
                else -> {
                }
            }
        }
        val edmDiff = EntityDataModel(
                Sets.newHashSet(),
                updatedSchemas,
                updatedEntityTypes,
                updatedAssociationTypes,
                updatedPropertyTypes)
        var conflicts: EntityDataModel? = null
        if (!conflictingPropertyTypes.isEmpty() || !conflictingEntityTypes.isEmpty()
                || !conflictingAssociationTypes.isEmpty()) {
            conflicts = EntityDataModel(
                    Sets.newHashSet(),
                    Sets.newHashSet(),
                    conflictingEntityTypes,
                    conflictingAssociationTypes,
                    conflictingPropertyTypes)
        }
        val diff = EntityDataModelDiff(edmDiff, Optional.ofNullable(conflicts))
        val cycles = cyclesAndConflicts[0]
        return Pair.of(diff, cycles)
    }

    private fun checkFqnDiffs(idToType: Map<UUID, FullQualifiedName>): List<Set<List<UUID?>>> {
        val conflictingIdsToFqns: MutableSet<UUID> = Sets.newHashSet()
        val updatedIdToFqn: MutableMap<UUID, FullQualifiedName?> = Maps.newHashMap()
        val internalFqnToId: SetMultimap<FullQualifiedName, UUID> = HashMultimap.create()
        val externalFqnToId: MutableMap<FullQualifiedName, UUID> = Maps.newHashMap()
        idToType.forEach { (id, fqn) ->
            val conflictId = aclKeys[fqn.toString()]
            updatedIdToFqn[id] = fqn
            internalFqnToId.put(fqn, id)
            conflictingIdsToFqns.add(id)
            if (conflictId != null) {
                externalFqnToId[fqn] = conflictId
            }
        }
        return resolveFqnCyclesLists(conflictingIdsToFqns, updatedIdToFqn, internalFqnToId, externalFqnToId)
    }

    private fun resolveFqnCyclesLists(
            conflictingIdsToFqns: MutableSet<UUID>,
            updatedIdToFqn: Map<UUID, FullQualifiedName?>,
            internalFqnToId: SetMultimap<FullQualifiedName, UUID>,
            externalFqnToId: Map<FullQualifiedName, UUID>): List<Set<List<UUID?>>> {
        val result: MutableSet<List<UUID?>> = Sets.newHashSet()
        val conflicts: MutableSet<List<UUID?>> = Sets.newHashSet()
        while (conflictingIdsToFqns.isNotEmpty()) {
            val initialId = conflictingIdsToFqns.first()
            val conflictingIdsViewed = mutableListOf<UUID?>()
            var id: UUID? = initialId
            var shouldReject = false
            var shouldResolve = false
            while (!shouldReject && !shouldResolve) {
                conflictingIdsViewed.add(0, id)
                val fqn = updatedIdToFqn[id]
                val idsForFqn = internalFqnToId[fqn]
                if (idsForFqn.size > 1) {
                    shouldReject = true
                } else {
                    id = externalFqnToId[fqn]
                    if (id == null || id == initialId) {
                        shouldResolve = true
                    } else if (!updatedIdToFqn
                                    .containsKey(id)) {
                        shouldReject = true
                    }
                }
            }
            if (shouldReject) {
                conflicts.add(conflictingIdsViewed)
            } else {
                result.add(conflictingIdsViewed)
            }
            conflictingIdsToFqns.removeAll(conflictingIdsViewed)
        }
        return Lists.newArrayList<Set<List<UUID?>>>(result, conflicts)
    }

    override fun getAllEntityTypePropertyMetadata(entityTypeId: UUID): Map<UUID, EntityTypePropertyMetadata> {
        return entityTypePropertyMetadata
                .entrySet(Predicates.equal(EntityTypePropertyMetadataMapstore.ENTITY_TYPE_INDEX, entityTypeId))
                .associate { it.key.propertyTypeId to it.value }
    }

    override fun getEntityTypePropertyMetadata(entityTypeId: UUID, propertyTypeId: UUID): EntityTypePropertyMetadata {
        val key = EntityTypePropertyKey(entityTypeId, propertyTypeId)
        if (!entityTypePropertyMetadata.containsKey(key)) {
            setUpDefaultEntityTypePropertyMetadata(entityTypeId)
        }
        return entityTypePropertyMetadata[key]!!
    }

    override fun updateEntityTypePropertyMetadata(entitySetId: UUID, propertyTypeId: UUID, update: MetadataUpdate) {
        val key = EntityTypePropertyKey(entitySetId, propertyTypeId)
        entityTypePropertyMetadata.executeOnKey(key, UpdateEntityTypePropertyMetadataProcessor(update))
    }

    override fun getEntityDataModel(): EntityDataModel {
        val schemas = schemaManager.allSchemas.sortedBy { it.fqn.toString() }
        val entityTypes = entityTypesStrict.sortedBy { it.type.toString() }
        val associationTypes = allAssociationTypes.sortedBy { it.associationEntityType.type.toString() }
        val propertyTypes = getPropertyTypes().sortedBy { it.type.toString() }
        val namespaces: MutableSet<String> = TreeSet()

        getEntityTypes().forEach { namespaces.add(it.type.namespace) }
        propertyTypes.forEach { namespaces.add(it.type.namespace) }

        return EntityDataModel(
                namespaces,
                schemas,
                entityTypes,
                associationTypes,
                propertyTypes)
    }

    override fun setEntityDataModel(edm: EntityDataModel) {
        val diffAndFqnCycles = getEntityDataModelDiffAndFqnLists(edm)
        val diff = diffAndFqnCycles.left
        val fqnCycles = diffAndFqnCycles.right
        require(!diff.conflicts.isPresent) { "Unable to update entity data model: please resolve conflicts before importing." }
        val idToType: MutableMap<UUID, SecurableObjectType> = Maps.newHashMap()
        val propertyTypesById: MutableMap<UUID, PropertyType> = Maps.newHashMap()
        val entityTypesById: MutableMap<UUID, EntityType> = Maps.newHashMap()
        val associationTypesById: MutableMap<UUID, AssociationType> = Maps.newHashMap()

        diff.diff.propertyTypes.forEach {
            idToType[it.id] = SecurableObjectType.PropertyTypeInEntitySet
            propertyTypesById[it.id] = it
        }
        diff.diff.entityTypes.forEach {
            idToType[it.id] = SecurableObjectType.EntityType
            entityTypesById[it.id] = it
        }
        diff.diff.associationTypes.forEach {
            idToType[it.associationEntityType.id] = SecurableObjectType.AssociationType
            associationTypesById[it.associationEntityType.id] = it
        }

        val updatedIds = mutableSetOf<UUID?>()
        fqnCycles.forEach { cycle ->
            cycle.forEach { id ->
                resolveFqnCycles(id,
                        idToType[id],
                        propertyTypesById,
                        entityTypesById,
                        associationTypesById)
                updatedIds.add(id)
            }
        }

        diff.diff.schemas.forEach { schemaManager.createOrUpdateSchemas(it) }
        diff.diff.propertyTypes.forEach {
            if (!updatedIds.contains(it.id)) {
                createOrUpdatePropertyType(it)
                eventBus.post(PropertyTypeCreatedEvent(it))
            }
        }
        diff.diff.entityTypes.forEach {
            if (!updatedIds.contains(it.id)) {
                createOrUpdateEntityType(it)
                eventBus.post(EntityTypeCreatedEvent(it))
            }
        }
        diff.diff.associationTypes.forEach {
            if (!updatedIds.contains(it.associationEntityType.id)) {
                createOrUpdateAssociationType(it)
                eventBus.post(AssociationTypeCreatedEvent(it))
            }
        }
    }

    override fun getAllLinkingEntitySetIdsForEntitySet(entitySetId: UUID): Set<UUID> {
        return entitySets.keySet(Predicates.equal(EntitySetMapstore.LINKED_ENTITY_SET_INDEX, entitySetId))
    }

    /* Entity set related functions */
    private fun getEntitySetIdsOfType(entityTypeId: UUID): Collection<UUID> {
        return entitySets.keySet(Predicates.equal(EntitySetMapstore.ENTITY_TYPE_ID_INDEX, entityTypeId))
    }

    private fun getEntitySetsOfType(entityTypeId: UUID): Collection<EntitySet> {
        return entitySets.values(Predicates.equal(EntitySetMapstore.ENTITY_TYPE_ID_INDEX, entityTypeId))
    }
}
