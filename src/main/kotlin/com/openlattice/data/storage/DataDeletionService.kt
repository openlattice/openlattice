package com.openlattice.data.storage

import com.google.common.collect.Iterables
import com.google.common.collect.Sets
import com.openlattice.IdConstants
import com.openlattice.auditing.AuditRecordEntitySetsManager
import com.openlattice.authorization.*
import com.openlattice.controllers.exceptions.ForbiddenException
import com.openlattice.data.*
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
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

/*
 * The general approach when deleting (hard or soft delete) entities is as follows:
 *
 * 1. Ensure the user has permissions to delete from the requested entity set.
 * 2. If not deleting from an association entity set, load all association entity set ids that are present in edges of
 *    the entities being deleted, and ensure the user is authorized to delete from all of those association entity sets.
 * 3. Delete all edges connected to the entities being deleted.
 * 4. If not deleting from an association entity set, also delete the association entities from all edges connected to
 *    the entities being deleted.
 * 5. Delete the entities themselves.
 *
 * We could end up with a zombie edge if a user creates an association between one of the deleted entities using a new
 * association entity set after the association auth checks are done. This theoretically shouldn't break anything,
 * but at some point we may want to introduce some kind of locking to prevent this behavior.
 *
 */

class DataDeletionService(
        private val edmService: EdmManager,
        private val entitySetManager: EntitySetManager,
        private val dgm: DataGraphManager,
        private val authorizationManager: AuthorizationManager,
        private val aresManager: AuditRecordEntitySetsManager,
        private val eds: EntityDatastore,
        private val graphService: GraphService
) : DataDeletionManager {

    companion object {
        const val MAX_BATCH_SIZE = 10_000
    }

    private val logger = LoggerFactory.getLogger(DataDeletionService::class.java)

    /** Delete all entities from an entity set **/

    override fun clearOrDeleteEntitySet(entitySetId: UUID, deleteType: DeleteType): WriteEvent {
        return clearOrDeleteEntitySet(entitySetId, deleteType, setOf(), skipAuthChecks = true)
    }

    override fun clearOrDeleteEntitySetIfAuthorized(
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

        // associations need to be deleted first, because edges are deleted when deleting entities in entity set
        clearOrDeleteAssociations(entitySetId, Optional.empty(), deleteType, principals, skipAuthChecks)

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


    override fun clearOrDeleteEntitiesIfAuthorized(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            deleteType: DeleteType,
            principals: Set<Principal>
    ): WriteEvent {
        return clearOrDeleteEntities(entitySetId, entityKeyIds, deleteType, principals, skipAuthChecks = false)
    }

    override fun clearOrDeleteEntitiesAndNeighborsIfAuthorized(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            srcEntitySetIds: Set<UUID>,
            dstEntitySetIds: Set<UUID>,
            deleteType: DeleteType,
            principals: Set<Principal>
    ): WriteEvent {

        // we don't include associations in filtering, since they will be deleted anyways with deleting the entities
        val filteringNeighborEntitySetIds = srcEntitySetIds + dstEntitySetIds

        // if no neighbor entity set ids are defined to delete from, it reduces down to a simple deleteEntities call
        if (filteringNeighborEntitySetIds.isEmpty()) {
            return clearOrDeleteEntities(entitySetId, entityKeyIds, deleteType, principals)
        }

        /*
         * 1 - Collect all neighbor entities, organized by EntitySet
         */

        val includeClearedEdges = deleteType == DeleteType.Hard

        val entitySetIdToEntityDataKeys: Map<UUID, Set<UUID>> = dgm
                .getEdgesConnectedToEntities(entitySetId, entityKeyIds, includeClearedEdges)
                .filter { edge ->
                    (edge.dst.entitySetId == entitySetId && filteringNeighborEntitySetIds.contains(edge.src.entitySetId))
                            || (edge.src.entitySetId == entitySetId && filteringNeighborEntitySetIds.contains(edge.dst.entitySetId))
                }
                .flatMap { edge -> setOf(edge.src, edge.dst) }
                .groupBy { it.entitySetId }
                .mapValues { it.value.map { it.entityKeyId }.toSet() }

        /*
        * 2 - Check authorization on all entities
        */

        /* Check entity set permissions */

        val authorizedPropertiesByEntitySets = getAuthorizedPropertyTypesForDeleteByEntitySet(
                setOf(entitySetId) + entitySetIdToEntityDataKeys.keys, // include original entity set too
                deleteType,
                Optional.empty(),
                principals
        )

        /* Check association permissions */

        // only original entity set can be association entity set
        val isAssociation = entitySetManager.isAssociationEntitySet(entitySetId)
        val authorizedAssociationPropertiesByEntitySets = if (isAssociation) {
            // no need to check associations on original entity set
            getAuthorizedPropertyTypesOfAssociations(entitySetIdToEntityDataKeys, deleteType, principals)
        } else {
            getAuthorizedPropertyTypesOfAssociations(
                    mapOf(entitySetId to entityKeyIds) + entitySetIdToEntityDataKeys,
                    deleteType,
                    principals
            )
        }


        /*
         * 3 - Delete all entities
         */

        /* Delete entity */

        var numUpdates = 0

        // associations need to be deleted first, because edges are deleted when deleting requested entities
        if (isAssociation) {
            // if entity set is association, we only delete its edges
            deleteEdgesForAssociationEntitySet(entitySetId, Optional.of(entityKeyIds), deleteType)
        } else {
            val associationNumUpdates = clearOrDeleteAuthorizedAssociations(
                    entitySetId,
                    Optional.of(entityKeyIds),
                    deleteType,
                    authorizedAssociationPropertiesByEntitySets.getValue(entitySetId)
            )
            numUpdates += associationNumUpdates
        }

        val entityWriteEvent = clearOrDeleteAuthorizedEntities(
                entitySetId,
                entityKeyIds,
                deleteType,
                authorizedPropertiesByEntitySets.getValue(entitySetId)
        )
        numUpdates += entityWriteEvent.numUpdates


        /* Delete neighbors */

        numUpdates += entitySetIdToEntityDataKeys.entries.stream().parallel().mapToInt { entry ->
            val neighborEntitySetId = entry.key
            val neighborEntityKeyIds = entry.value

            // associations need to be deleted first, because edges are deleted when deleting requested entities
            val associationNumUpdates = clearOrDeleteAuthorizedAssociations(
                    neighborEntitySetId,
                    Optional.of(neighborEntityKeyIds),
                    deleteType,
                    authorizedAssociationPropertiesByEntitySets.getValue(neighborEntitySetId))

            val neighborWriteEvent = clearOrDeleteAuthorizedEntities(
                    neighborEntitySetId,
                    neighborEntityKeyIds,
                    deleteType,
                    authorizedPropertiesByEntitySets.getValue(neighborEntitySetId))

            neighborWriteEvent.numUpdates + associationNumUpdates
        }.sum()

        return WriteEvent(entityWriteEvent.version, numUpdates)
    }

    /**
     * Deletes specific entities from an entity set. If skipAuthChecks is set to true, it will not check if entity set
     * is an audit entity set or not nor whether the requesting principals are authorized to delete from entity set.
     */
    private fun clearOrDeleteEntities(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            deleteType: DeleteType,
            principals: Set<Principal>,
            skipAuthChecks: Boolean = false
    ): WriteEvent {

        // access checks for entity set and properties
        val authorizedPropertyTypes = getAuthorizedPropertyTypesForDelete(
                entitySetId,
                Optional.empty(),
                deleteType,
                principals,
                skipAuthChecks)

        // associations need to be deleted first, because edges are deleted when deleting requested entities in entity set
        clearOrDeleteAssociations(entitySetId, Optional.of(entityKeyIds), deleteType, principals, skipAuthChecks)

        return clearOrDeleteAuthorizedEntities(entitySetId, entityKeyIds, deleteType, authorizedPropertyTypes)
    }

    private fun clearOrDeleteAuthorizedEntities(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            deleteType: DeleteType,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        var numUpdates = 0
        var maxVersion = Long.MIN_VALUE

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

    /**
     * Delete property values from specific entities. No entities are actually deleted here.
     */
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
            eds.clearEntityProperties(entitySetId, entityKeyIds, authorizedPropertyTypes)
        }

        logger.info(
                "Deleted properties {} of {} entities.",
                authorizedPropertyTypes.values.map(PropertyType::getType), writeEvent.numUpdates
        )

        return writeEvent

    }

    /* Association deletion and auth */

    /**
     * Checks authorizations on connected association entities and clears/deletes them along with edges connected to
     * them.
     * If entity set is an association entity set, it only deletes its edges.
     */
    private fun clearOrDeleteAssociations(
            entitySetId: UUID,
            entityKeyIds: Optional<Set<UUID>>,
            deleteType: DeleteType,
            principals: Set<Principal>,
            skipAuthChecks: Boolean = false
    ): Int {

        // if this is an association entity set, we only delete the edges
        if (entitySetManager.isAssociationEntitySet(entitySetId)) {
            deleteEdgesForAssociationEntitySet(entitySetId, entityKeyIds, deleteType)
            return 0
        }

        // if it's not an association entity set, we delete edges + connected association entities
        val authorizedPropertyTypes = getAuthorizedPropertyTypesOfAssociations(
                entitySetId,
                entityKeyIds,
                deleteType,
                principals,
                skipAuthChecks)

        return clearOrDeleteAuthorizedAssociations(entitySetId, entityKeyIds, deleteType, authorizedPropertyTypes)
    }

    /**
     * Deletes or clears connected association entities of entities in entity set filtered optionally by [entityKeyIds].
     */
    private fun clearOrDeleteAuthorizedAssociations(
            entitySetId: UUID,
            entityKeyIds: Optional<Set<UUID>>,
            deleteType: DeleteType,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>
    ): Int {
        val associationsEdgeKeys = collectAssociations(
                entitySetId,
                entityKeyIds,
                includeClearedEdges = (deleteType == DeleteType.Hard)
        )

        val writeEvents = if (deleteType === DeleteType.Hard) {
            dgm.deleteAssociationsBatch(entitySetId, associationsEdgeKeys, authorizedPropertyTypes)
        } else {
            val auditEntitySetIds = aresManager.getAuditEdgeEntitySets(AclKey(entitySetId))
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
        val edgeKeys = collectAssociations(
                entitySetId,
                entityKeyIds,
                includeClearedEdges = deleteType == DeleteType.Hard
        )

        if (deleteType == DeleteType.Hard) {
            graphService.deleteEdges(edgeKeys)
        } else {
            graphService.clearEdges(edgeKeys)
        }
    }

    /* Helpers */

    /**
     * Returns all DataEdgeKeys where either src, dst and/or edge entity set ids are equal the requested entitySetId
     * and if entityKeyIds are provided, it matches them also against one or more of src,dst and edge entity key id.
     * If includeClearedEdges is set to true, it will also return cleared (version < 0) entities.
     */
    private fun collectAssociations(
            entitySetId: UUID,
            entityKeyIds: Optional<Set<UUID>>,
            includeClearedEdges: Boolean): PostgresIterable<DataEdgeKey> {
        return if (entityKeyIds.isPresent)
            dgm.getEdgesConnectedToEntities(entitySetId, entityKeyIds.get(), includeClearedEdges)
        else
            dgm.getEdgeKeysOfEntitySet(entitySetId, includeClearedEdges)
    }


    /* Authorization checks */

    private fun getAuthorizedPropertyTypesOfAssociations(
            entityKeyIdsBySetId: Map<UUID, Set<UUID>>,
            deleteType: DeleteType,
            principals: Set<Principal>
    ): Map<UUID, Map<UUID, Map<UUID, PropertyType>>> {
        return entityKeyIdsBySetId.asSequence().map {
            val entityKeyIds = Optional.of(it.value)
            it.key to getAuthorizedPropertyTypesOfAssociations(it.key, entityKeyIds, deleteType, principals)
        }.toMap()
    }

    private fun getAuthorizedPropertyTypesOfAssociations(
            entitySetId: UUID,
            entityKeyIds: Optional<Set<UUID>>,
            deleteType: DeleteType,
            principals: Set<Principal>,
            skipAuthChecks: Boolean = false
    ): Map<UUID, Map<UUID, PropertyType>> {

        val edgeEntitySetIds = if (entityKeyIds.isPresent) {
            dgm.getEdgeEntitySetsConnectedToEntities(entitySetId, entityKeyIds.get())
        } else {
            dgm.getEdgeEntitySetsConnectedToEntitySet(entitySetId)
        }

        val edgeEntitySets = entitySetManager.getEntitySetsAsMap(edgeEntitySetIds).values
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

        return entitySetsToDeleteFrom.associate {
            it.id to entityTypesById.getValue(it.entityTypeId).properties.associateWith { ptId -> propertyTypesById.getValue(ptId) }.toMap()
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
                    "$requiredPermissions on associations for AclKeys $unauthorizedAclKeys")
        }

    }

    private fun getAuthorizedPropertyTypesForDeleteByEntitySet(
            entitySetIds: Set<UUID>,
            deleteType: DeleteType,
            properties: Optional<Set<UUID>>,
            principals: Set<Principal>
    ): Map<UUID, Map<UUID, PropertyType>> {

        val entitySets = entitySetManager.getEntitySetsAsMap(entitySetIds).values
        val requiredPermissions = getRequiredPermissions(deleteType)

        entitySets.forEach {
            if (it.flags.contains(EntitySetFlag.AUDIT)) {
                throw ForbiddenException("Cannot delete from entity set ${it.id} because it is an audit entity set.")
            }
        }

        val entityTypesById = edmService.getEntityTypesAsMap(entitySets.map { it.entityTypeId }.toSet())

        val accessChecks = mutableSetOf<AccessCheck>()

        /* Ignore audit entity sets when determining delete or clear permissions */
        entitySets.forEach {
            accessChecks.add(AccessCheck(AclKey(it.id), requiredPermissions))

            val propertyTypeIds = properties.orElse(entityTypesById.getValue(it.entityTypeId).properties)
            propertyTypeIds.forEach { propertyTypeId ->
                accessChecks.add(AccessCheck(AclKey(it.id, propertyTypeId), requiredPermissions))
            }
        }

        val unauthorizedAclKeys = authorizationManager.accessChecksForPrincipals(accessChecks, principals)
                .filter { it.permissions.values.contains(false) }
                .map { it.aclKey }
                .collect(Collectors.toSet())

        if (unauthorizedAclKeys.isNotEmpty()) {
            throw ForbiddenException("Unable to delete from entity sets $entitySetIds: missing required permissions " +
                    "$requiredPermissions for AclKeys $unauthorizedAclKeys")
        }

        val allPropertyTypeIds = properties.orElse(entityTypesById.values.flatMap { it.properties }.toSet())
        val propertyTypesById = edmService.getPropertyTypesAsMap(allPropertyTypeIds)

        return entitySets.associate {
            it.id to properties.orElse(entityTypesById.getValue(it.entityTypeId).properties)
                    .associateWith { ptId -> propertyTypesById.getValue(ptId) }.toMap()
                    .plus(IdConstants.ID_ID.id to PostgresMetaDataProperties.ID.propertyType)
        }.toMap()

    }

    private fun getAuthorizedPropertyTypesForDelete(
            entitySetId: UUID,
            properties: Optional<Set<UUID>>,
            deleteType: DeleteType,
            principals: Set<Principal>,
            skipAuthChecks: Boolean = false
    ): Map<UUID, PropertyType> {

        if (skipAuthChecks) {
            return entitySetManager.getPropertyTypesForEntitySet(entitySetId)
        }

        return getAuthorizedPropertyTypesForDeleteByEntitySet(
                setOf(entitySetId),
                deleteType,
                properties,
                principals
        ).getValue(entitySetId)
    }

    private fun getRequiredPermissions(deleteType: DeleteType): EnumSet<Permission> {
        return if (deleteType === DeleteType.Hard) {
            EnumSet.of(Permission.OWNER)
        } else {
            EnumSet.of(Permission.WRITE)
        }
    }

}