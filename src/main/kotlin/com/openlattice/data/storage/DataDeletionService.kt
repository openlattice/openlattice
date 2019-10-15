package com.openlattice.data.storage

import com.google.common.collect.ImmutableSet
import com.google.common.collect.Iterables
import com.google.common.collect.Sets
import com.openlattice.IdConstants
import com.openlattice.auditing.AuditRecordEntitySetsManager
import com.openlattice.authorization.*
import com.openlattice.controllers.exceptions.ForbiddenException
import com.openlattice.data.*
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.core.GraphService
import com.openlattice.postgres.PostgresMetaDataProperties
import com.openlattice.postgres.streams.PostgresIterable
import org.slf4j.LoggerFactory
import java.util.*
import java.util.stream.Collectors
import kotlin.math.max

class DataDeletionService(
        private val edmService: EdmManager,
        private val dgm: DataGraphManager,
        private val authzHelper: EdmAuthorizationHelper,
        private val authorizationManager: AuthorizationManager,
        private val aresManager: AuditRecordEntitySetsManager,
        private val eds: EntityDatastore,
        private val graphService: GraphService
) : DataDeletionManager {

    companion object {
        const val MAX_BATCH_SIZE = 10_000
    }

    private val logger = LoggerFactory.getLogger(DataDeletionService::class.java)

    override fun clearOrDeleteEntitySet(entitySetId: UUID, deleteType: DeleteType): WriteEvent {
        return clearOrDeleteEntitySet(entitySetId, deleteType, setOf(), skipAuthChecks = true)
    }

    override fun clearOrDeleteEntitySet(
            entitySetId: UUID,
            deleteType: DeleteType,
            principals: Set<Principal>
    ): WriteEvent {
        return clearOrDeleteEntitySet(entitySetId, deleteType, principals, skipAuthChecks = false)
    }

    private fun clearOrDeleteEntitySet(
            entitySetId: UUID,
            deleteType: DeleteType,
            principals: Set<Principal>,
            skipAuthChecks: Boolean = false
    ): WriteEvent {

        val authorizedPropertyTypes = getAuthorizedPropertyTypesForDelete(
                entitySetId,
                Optional.empty(),
                deleteType,
                principals,
                skipAuthChecks
        )

        authorizeAndDeleteAssociations(entitySetId, Optional.empty(), deleteType, principals, skipAuthChecks)

        val writeEvent = if (deleteType == DeleteType.Hard) {
            eds.deleteEntitySetData(entitySetId, authorizedPropertyTypes)
        } else {
            eds.clearEntitySet(entitySetId, authorizedPropertyTypes)
        }

        logger.info("Deleted {} entities from entity set {}.", writeEvent.numUpdates, entitySetId)

        return writeEvent

    }

    override fun clearOrDeleteEntities(entitySetId: UUID, entityKeyIds: Set<UUID>, deleteType: DeleteType): WriteEvent {
        return clearOrDeleteEntities(entitySetId, entityKeyIds, deleteType, setOf(), skipAuthChecks = true)
    }


    override fun clearOrDeleteEntities(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            deleteType: DeleteType,
            principals: Set<Principal>
    ): WriteEvent {
        return clearOrDeleteEntities(entitySetId, entityKeyIds, deleteType, principals, skipAuthChecks = false)
    }

    private fun clearOrDeleteEntities(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            deleteType: DeleteType,
            principals: Set<Principal>,
            skipAuthChecks: Boolean = false
    ): WriteEvent {
        var numUpdates = 0
        var maxVersion = java.lang.Long.MIN_VALUE

        // access checks for entity set and properties
        val authorizedPropertyTypes = getAuthorizedPropertyTypesForDelete(
                entitySetId,
                Optional.empty(),
                deleteType,
                principals,
                skipAuthChecks)

        authorizeAndDeleteAssociations(entitySetId, Optional.of(entityKeyIds), deleteType, principals, skipAuthChecks)

        val entityKeyIdChunks = Iterables.partition(entityKeyIds, MAX_BATCH_SIZE)
        for (chunkList in entityKeyIdChunks) {
            val chunk = Sets.newHashSet(chunkList)

            val writeEvent = if (deleteType === DeleteType.Hard) {
                eds.deleteEntities(entitySetId, chunk, authorizedPropertyTypes)
            } else {
                eds.clearEntities(entitySetId, chunk, authorizedPropertyTypes)
            }

            numUpdates += writeEvent.numUpdates
            maxVersion = max(maxVersion, writeEvent.version)
        }

        logger.info("Deleted {} entities from entity set {}.", numUpdates, entitySetId)

        return WriteEvent(maxVersion, numUpdates)
    }

    override fun clearOrDeleteEntityProperties(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            deleteType: DeleteType,
            propertyTypeIds: Set<UUID>,
            principals: Set<Principal>
    ): WriteEvent {

        val authorizedPropertyTypes = getAuthorizedPropertyTypesForDelete(
                entitySetId,
                Optional.of(propertyTypeIds),
                deleteType,
                principals)

        val writeEvent = if (deleteType === DeleteType.Hard) {
            eds.deleteEntityProperties(entitySetId, entityKeyIds, authorizedPropertyTypes)
        } else {
            eds.clearEntityData(entitySetId, entityKeyIds, authorizedPropertyTypes)
        }

        logger.info(
                "Deleted properties {} of {} entities.",
                authorizedPropertyTypes.values.map(PropertyType::getType), writeEvent.numUpdates
        )

        return writeEvent

    }

    private fun authorizeAndDeleteAssociations(
            entitySetId: UUID,
            entityKeyIds: Optional<Set<UUID>>,
            deleteType: DeleteType,
            principals: Set<Principal>,
            skipAuthChecks: Boolean = false
    ): Int {

        if (!edmService.isAssociationEntitySet(entitySetId)) {
            deleteEdgesForAssociationEntitySet(entitySetId, entityKeyIds, deleteType)
            return 0
        }

        val authorizedPropertyTypes = getAuthorizedPropertyTypesOfAssociations(
                entitySetId,
                entityKeyIds,
                deleteType,
                principals,
                skipAuthChecks)

        val auditEntitySetIds = aresManager.getAuditEdgeEntitySets(AclKey(entitySetId))

        val associationsEdgeKeys = collectAssociations(entitySetId,
                entityKeyIds,
                includeClearedEdges = deleteType == DeleteType.Hard)

        val writeEvents = mutableListOf<WriteEvent>()

        writeEvents += if (deleteType === DeleteType.Hard) {
            // associations need to be deleted first, because edges are deleted in DataGraphManager.deleteEntitySet call
            dgm.deleteAssociationsBatch(entitySetId, associationsEdgeKeys, authorizedPropertyTypes)
        } else {
            val nonAuditEdgeKeys = associationsEdgeKeys.filter { !auditEntitySetIds.contains(it.edge.entitySetId) }
            dgm.clearAssociationsBatch(entitySetId, nonAuditEdgeKeys, authorizedPropertyTypes)
        }

        return writeEvents.sumBy { it.numUpdates }

    }

    private fun deleteEdgesForAssociationEntitySet(
        entitySetId: UUID,
        entityKeyIds: Optional<Set<UUID>>,
        deleteType: DeleteType
    ) {
        val edgeKeys = collectAssociations(entitySetId,
                entityKeyIds,
                includeClearedEdges = deleteType == DeleteType.Hard)

        if (deleteType == DeleteType.Hard) {
            graphService.deleteEdges(edgeKeys)
        } else {
            graphService.clearEdges(edgeKeys)
        }
    }

    /** Helpers **/

    private fun collectAssociations(
            entitySetId: UUID,
            entityKeyIds: Optional<Set<UUID>>,
            includeClearedEdges: Boolean): PostgresIterable<DataEdgeKey> {
        return if (entityKeyIds.isPresent)
            dgm.getEdgesConnectedToEntities(entitySetId, entityKeyIds.get(), includeClearedEdges)
        else
            dgm.getEdgeKeysOfEntitySet(entitySetId)
    }


    /** Authorization checks **/

    private fun getAuthorizedPropertyTypesForDelete(
            entitySetId: UUID,
            properties: Optional<Set<UUID>>,
            deleteType: DeleteType,
            principals: Set<Principal>,
            skipAuthChecks: Boolean = false
    ): Map<UUID, PropertyType> {
        val entitySet = edmService.getEntitySet(entitySetId)
        return getAuthorizedPropertyTypesForDelete(entitySet, properties, deleteType, principals, skipAuthChecks)
    }

    private fun checkPermissions(aclKey: AclKey, requiredPermissions: EnumSet<Permission>, principals: Set<Principal>) {
        if (!authorizationManager.checkIfHasPermissions(aclKey, principals, requiredPermissions)) {
            throw ForbiddenException("Object $aclKey is not accessible: required permissions are $requiredPermissions")
        }
    }

    private fun getAuthorizedPropertyTypesOfAssociations(
            entitySetId: UUID,
            entityKeyIds: Optional<Set<UUID>>,
            deleteType: DeleteType,
            principals: Set<Principal>,
            skipAuthChecks: Boolean
    ): Map<UUID, Map<UUID, PropertyType>> {

        val edgeEntitySetIds = if (entityKeyIds.isPresent) {
            dgm.getEdgeEntitySetsConnectedToEntities(entitySetId, entityKeyIds.get())
        } else {
            dgm.getEdgeEntitySetsConnectedToEntitySet(entitySetId)
        }

        val edgeEntitySets = edmService.getEntitySetsAsMap(edgeEntitySetIds).values
        val nonAuditEdgeEntitySets = edgeEntitySets.filter { !it.flags.contains(EntitySetFlag.AUDIT) }
        val entityTypesById = edmService.getEntityTypesAsMap(edgeEntitySets.map { it.entityTypeId }.toSet())

        if (!skipAuthChecks) {
            authorizeAssociations(entitySetId, nonAuditEdgeEntitySets, entityTypesById, deleteType, principals)
        }

        /* Authorizations successful. Return map from entity set ids to property types */

        val allPropertyTypeIds = entityTypesById.values.flatMap { it.properties }.toSet()
        val propertyTypesById = edmService.getPropertyTypesAsMap(allPropertyTypeIds)


        /* Hard deletes will authorize deletion of all audit property types, while soft deletes will authorize none. */
        val entitySetsToDeleteFrom = if (deleteType == DeleteType.Hard) edgeEntitySets else nonAuditEdgeEntitySets

        return entitySetsToDeleteFrom.map { it.id }.associateWith {
            entityTypesById.getValue(it).properties.associateWith { ptId -> propertyTypesById.getValue(ptId) }.toMap()
                    .plus(IdConstants.ID_ID.id to PostgresMetaDataProperties.ID.propertyType)
        }.toMap()
    }

    private fun authorizeAssociations(
            entitySetId: UUID,
            edgeEntitySets: List<EntitySet>,
            entityTypesById: Map<UUID, EntityType>,
            deleteType: DeleteType,
            principals: Set<Principal>
    ) {
        val requiredPermissions = getRequiredPermissions(deleteType)

        val accessChecks = mutableSetOf<AccessCheck>()

        /* Ignore audit entity sets when determining delete or clear permissions */
        edgeEntitySets.forEach {
            accessChecks.add(AccessCheck(AclKey(it.id), requiredPermissions))

            val propertyTypeIds = entityTypesById.getValue(it.entityTypeId).properties
            propertyTypeIds.forEach { propertyTypeId ->
                accessChecks.add(AccessCheck(AclKey(it.id, propertyTypeId), requiredPermissions))
            }
        }

        val unauthorizedAclKeys = authorizationManager.accessChecksForPrincipals(accessChecks, principals)
                .filter { it.permissions.values.contains(false) }
                .map { it.aclKey }
                .collect(Collectors.toSet())

        if (unauthorizedAclKeys.isNotEmpty()) {
            throw ForbiddenException("Unable to delete from entity set $entitySetId: missing required permissions " +
                    "on associations for AclKeys $unauthorizedAclKeys")
        }

    }

    private fun getAuthorizedPropertyTypesForDelete(
            entitySet: EntitySet,
            properties: Optional<Set<UUID>>,
            deleteType: DeleteType,
            principals: Set<Principal>,
            skipAuthChecks: Boolean = false
    ): Map<UUID, PropertyType> {
        val entitySetId = entitySet.id

        if (skipAuthChecks) {
            return edmService.getPropertyTypesForEntitySet(entitySetId)
        }

        val permissionsToCheck = getRequiredPermissions(deleteType)
        checkPermissions(AclKey(entitySetId), permissionsToCheck, principals)

        if (entitySet.isLinking) {
            throw IllegalArgumentException("You cannot delete entities from a linking entity set.")
        }

        val entityType = edmService.getEntityType(entitySet.entityTypeId)
        val requiredProperties = properties.orElse(entityType.properties)
        val authorizedPropertyTypes = authzHelper
                .getAuthorizedPropertyTypes(ImmutableSet.of<UUID>(entitySetId),
                        requiredProperties,
                        permissionsToCheck,
                        principals)
                .getValue(entitySetId)

        if (!authorizedPropertyTypes.keys.containsAll(requiredProperties)) {
            throw ForbiddenException(
                    "You must have ${permissionsToCheck.iterator().next()} permission of all required " +
                            "entity set ${entitySet.id} properties to delete entities from it.")
        }

        // if we delete all properties, also delete @id
        if (properties.isEmpty) {
            authorizedPropertyTypes[IdConstants.ID_ID.id] = PostgresMetaDataProperties.ID.propertyType
        }

        return authorizedPropertyTypes
    }

    private fun getRequiredPermissions(deleteType: DeleteType): EnumSet<Permission> {
        return if (deleteType === DeleteType.Hard) {
            EnumSet.of(Permission.OWNER)
        } else {
            EnumSet.of(Permission.WRITE)
        }
    }

}