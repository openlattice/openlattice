package com.openlattice.collection

import com.openlattice.authorization.*
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.collections.CollectionsApi
import com.openlattice.collections.CollectionsApi.Companion.AUTO_CREATE
import com.openlattice.collections.CollectionsApi.Companion.CONTROLLER
import com.openlattice.collections.CollectionsApi.Companion.ENTITY_SET_COLLECTION_ID
import com.openlattice.collections.CollectionsApi.Companion.ENTITY_SET_COLLECTION_ID_PATH
import com.openlattice.collections.CollectionsApi.Companion.ENTITY_SET_PATH
import com.openlattice.collections.CollectionsApi.Companion.ENTITY_TYPE_COLLECTION_ID
import com.openlattice.collections.CollectionsApi.Companion.ENTITY_TYPE_COLLECTION_ID_PATH
import com.openlattice.collections.CollectionsApi.Companion.ENTITY_TYPE_PATH
import com.openlattice.collections.CollectionsApi.Companion.TEMPLATE_PATH
import com.openlattice.collections.CollectionsApi.Companion.TYPE_ID
import com.openlattice.collections.CollectionsApi.Companion.TYPE_ID_PATH
import com.openlattice.collections.CollectionsManager
import com.openlattice.controllers.exceptions.ForbiddenException
import com.openlattice.edm.PostgresEdmManager
import com.openlattice.edm.collection.CollectionTemplateType
import com.openlattice.edm.collection.EntitySetCollection
import com.openlattice.edm.collection.EntityTypeCollection
import com.openlattice.edm.requests.MetadataUpdate
import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.*
import java.util.*
import java.util.stream.Collectors
import javax.inject.Inject
import kotlin.streams.toList

@RestController
@RequestMapping(CONTROLLER)
class CollectionsController : CollectionsApi, AuthorizingComponent {

    companion object {
        private val logger = LoggerFactory.getLogger(CollectionsController::class.java)!!
    }

    @Inject
    private lateinit var authorizationManager: AuthorizationManager

    @Inject
    private lateinit var postgresEdmManager: PostgresEdmManager

    @Inject
    private lateinit var collectionsManager: CollectionsManager

    @RequestMapping(path = [ENTITY_TYPE_PATH], method = [RequestMethod.GET])
    override fun getAllEntityTypeCollections(): Iterable<EntityTypeCollection> {
        return collectionsManager.getAllEntityTypeCollections()
    }

    @RequestMapping(path = [ENTITY_SET_PATH], method = [RequestMethod.GET])
    override fun getAllEntitySetCollections(): Iterable<EntitySetCollection> {
        return collectionsManager.getEntitySetCollections(getAuthorizedEntitySetCollectionIds()).values
    }

    @RequestMapping(path = [ENTITY_TYPE_PATH + ENTITY_TYPE_COLLECTION_ID_PATH], method = [RequestMethod.GET])
    override fun getEntityTypeCollection(@PathVariable(ENTITY_TYPE_COLLECTION_ID) entityTypeCollectionId: UUID): EntityTypeCollection {
        return collectionsManager.getEntityTypeCollection(entityTypeCollectionId)
    }

    @RequestMapping(path = [ENTITY_SET_PATH + ENTITY_SET_COLLECTION_ID_PATH], method = [RequestMethod.GET])
    override fun getEntitySetCollection(@PathVariable(ENTITY_SET_COLLECTION_ID) entitySetCollectionId: UUID): EntitySetCollection {
        ensureReadAccess(AclKey(entitySetCollectionId))
        return collectionsManager.getEntitySetCollection(entitySetCollectionId)
    }

    @RequestMapping(path = [ENTITY_SET_PATH + ENTITY_TYPE_PATH + ENTITY_TYPE_COLLECTION_ID_PATH], method = [RequestMethod.GET])
    override fun getEntitySetCollectionsOfType(@PathVariable(ENTITY_TYPE_COLLECTION_ID) entityTypeCollectionId: UUID): Iterable<EntitySetCollection> {
        return collectionsManager.getEntitySetCollectionsOfType(getAuthorizedEntitySetCollectionIds(), entityTypeCollectionId)
    }

    @RequestMapping(path = [ENTITY_TYPE_PATH], method = [RequestMethod.POST])
    override fun createEntityTypeCollection(@RequestBody entityTypeCollection: EntityTypeCollection): UUID {
        ensureAdminAccess()
        return collectionsManager.createEntityTypeCollection(entityTypeCollection)
    }

    @RequestMapping(path = [ENTITY_SET_PATH], method = [RequestMethod.POST])
    override fun createEntitySetCollection(
            @RequestBody entitySetCollection: EntitySetCollection,
            @RequestParam(value = AUTO_CREATE, defaultValue = "true") autoCreate: Boolean): UUID {
        val unauthorizedEntitySetIds = findUnauthorizedTemplateEntitySets(entitySetCollection.template)
        if (unauthorizedEntitySetIds.isNotEmpty()) {
            throw ForbiddenException("Unable to create EntitySetCollection ${entitySetCollection.name} because entity sets $unauthorizedEntitySetIds are not authorized")
        }

        return collectionsManager.createEntitySetCollection(entitySetCollection, autoCreate)
    }

    @RequestMapping(path = [ENTITY_TYPE_PATH + ENTITY_TYPE_COLLECTION_ID_PATH], method = [RequestMethod.PATCH])
    override fun updateEntityTypeCollectionMetadata(
            @PathVariable(ENTITY_TYPE_COLLECTION_ID) entityTypeCollectionId: UUID,
            @RequestBody metadataUpdate: MetadataUpdate) {
        ensureAdminAccess()
        collectionsManager.updateEntityTypeCollectionMetadata(entityTypeCollectionId, metadataUpdate)
    }

    @RequestMapping(path = [ENTITY_TYPE_PATH + ENTITY_TYPE_COLLECTION_ID_PATH + TEMPLATE_PATH], method = [RequestMethod.PATCH])
    override fun addTypeToEntityTypeCollectionTemplate(
            @PathVariable(ENTITY_TYPE_COLLECTION_ID) entityTypeCollectionId: UUID,
            @RequestBody collectionTemplateType: CollectionTemplateType) {
        ensureAdminAccess()

        collectionsManager.addTypeToEntityTypeCollectionTemplate(entityTypeCollectionId, collectionTemplateType)
    }

    @RequestMapping(path = [ENTITY_TYPE_PATH + ENTITY_TYPE_COLLECTION_ID_PATH + TEMPLATE_PATH + TYPE_ID_PATH], method = [RequestMethod.DELETE])
    override fun removeTypeFromEntityTypeCollectionTemplate(
            @PathVariable(ENTITY_TYPE_COLLECTION_ID) entityTypeCollectionId: UUID,
            @PathVariable(TYPE_ID) typeId: UUID) {
        ensureAdminAccess()

        collectionsManager.removeKeyFromEntityTypeCollectionTemplate(entityTypeCollectionId, typeId)
    }

    @RequestMapping(path = [ENTITY_SET_PATH + ENTITY_SET_COLLECTION_ID_PATH], method = [RequestMethod.PATCH])
    override fun updateEntitySetCollectionMetadata(
            @PathVariable(ENTITY_SET_COLLECTION_ID) entitySetCollectionId: UUID,
            @RequestBody metadataUpdate: MetadataUpdate) {
        ensureOwnerAccess(AclKey(entitySetCollectionId))

        collectionsManager.updateEntitySetCollectionMetadata(entitySetCollectionId, metadataUpdate)
    }

    @RequestMapping(path = [ENTITY_SET_PATH + ENTITY_SET_COLLECTION_ID_PATH + TEMPLATE_PATH], method = [RequestMethod.PATCH])
    override fun updateEntitySetCollectionTemplate(
            @PathVariable(ENTITY_SET_COLLECTION_ID) entitySetCollectionId: UUID,
            @RequestBody templateUpdates: Map<UUID, UUID>) {
        ensureOwnerAccess(AclKey(entitySetCollectionId))

        val unauthorizedEntitySetIds = findUnauthorizedTemplateEntitySets(templateUpdates)
        if (unauthorizedEntitySetIds.isNotEmpty()) {
            throw ForbiddenException("Unable to update EntitySetCollection $entitySetCollectionId template because entity sets $unauthorizedEntitySetIds are not authorized")
        }

        collectionsManager.updateEntitySetCollectionTemplate(entitySetCollectionId, templateUpdates)
    }

    @RequestMapping(path = [ENTITY_TYPE_PATH + ENTITY_TYPE_COLLECTION_ID_PATH], method = [RequestMethod.DELETE])
    override fun deleteEntityTypeCollection(@PathVariable(ENTITY_TYPE_COLLECTION_ID) entityTypeCollectionId: UUID) {
        ensureAdminAccess()
        collectionsManager.deleteEntityTypeCollection(entityTypeCollectionId)
    }

    @RequestMapping(path = [ENTITY_SET_PATH + ENTITY_SET_COLLECTION_ID_PATH], method = [RequestMethod.DELETE])
    override fun deleteEntitySetCollection(@PathVariable(ENTITY_SET_COLLECTION_ID) entitySetCollectionId: UUID) {
        ensureOwnerAccess(AclKey(entitySetCollectionId))
        collectionsManager.deleteEntitySetCollection(entitySetCollectionId)
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }

    private fun findUnauthorizedTemplateEntitySets(template: Map<UUID, UUID>): List<UUID> {
        return authorizationManager.accessChecksForPrincipals(
                template.values.map { AccessCheck(AclKey(it), EnumSet.of(Permission.READ)) }.toSet(),
                Principals.getCurrentPrincipals())
                .filter { !it.permissions[Permission.READ]!! }
                .map { it.aclKey[0] }
                .toList()
    }

    private fun getAuthorizedEntitySetCollectionIds(): Set<UUID> {
        return authorizationManager
                .getAuthorizedObjectsOfType(Principals.getCurrentPrincipals(), SecurableObjectType.EntitySetCollection, EnumSet.of(Permission.READ))
                .map { it[0] }.collect(Collectors.toSet())
    }
}