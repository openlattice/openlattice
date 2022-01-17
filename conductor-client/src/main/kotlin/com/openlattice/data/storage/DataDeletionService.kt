package com.openlattice.data.storage

import com.codahale.metrics.annotation.Timed
import com.geekbeast.rhizome.jobs.HazelcastJobService
import com.openlattice.authorization.AccessCheck
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.Permission
import com.openlattice.authorization.Principal
import com.geekbeast.controllers.exceptions.ForbiddenException
import com.openlattice.data.DataDeletionManager
import com.openlattice.data.DeleteType
import com.openlattice.data.WriteEvent
import com.openlattice.data.jobs.DataDeletionJob
import com.openlattice.data.jobs.DataDeletionJobState
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.core.GraphService
import com.openlattice.search.requests.EntityNeighborsFilter
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*

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

@Service
class DataDeletionService(
        private val entitySetManager: EntitySetManager,
        private val authorizationManager: AuthorizationManager,
        private val eds: EntityDatastore,
        private val graphService: GraphService,
        private val jobService: HazelcastJobService,
) : DataDeletionManager {

    companion object {
        @JvmStatic
        val PERMISSIONS_FOR_DELETE_TYPE = mapOf(
                DeleteType.Soft to EnumSet.of(Permission.WRITE),
                DeleteType.Hard to EnumSet.of(Permission.OWNER)
        )
    }

    private val logger = LoggerFactory.getLogger(DataDeletionService::class.java)

    /** Delete all entities from an entity set **/

    @Timed
    override fun clearOrDeleteEntitySet(entitySetId: UUID, deleteType: DeleteType): UUID {

        return jobService.submitJob(DataDeletionJob(DataDeletionJobState(
                entitySetId,
                deleteType,
        )))

    }

    @Timed
    override fun clearOrDeleteEntities(entitySetId: UUID, entityKeyIds: MutableSet<UUID>, deleteType: DeleteType): UUID {

        return jobService.submitJob(DataDeletionJob(DataDeletionJobState(
                entitySetId,
                deleteType,
                entityKeyIds
        )))
    }

    /**
     * Delete property values from specific entities. No entities are actually deleted here.
     */
    @Timed
    override fun clearOrDeleteEntityProperties(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            deleteType: DeleteType,
            propertyTypeIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {

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

    /* Authorization checks */

    @Timed
    override fun authCheckForEntitySetsAndNeighbors(entitySetIds: Set<UUID>, deleteType: DeleteType, principals: Set<Principal>, entityKeyIds: Set<UUID>?) {

        val srcDstEntitySets = mutableSetOf<UUID>()
        entitySetIds.forEach { esid ->
            entitySetManager.getEntitySet(esid)?.let {
                if (!it.flags.contains(EntitySetFlag.ASSOCIATION)) {
                    srcDstEntitySets.add(esid)
                }
            } ?: run {
                throw IllegalArgumentException("Unable to perform delete on $entitySetIds because $esid does not exist")
            }
        }

        val neighborEdgeEntitySets = if (srcDstEntitySets.isEmpty()) mutableSetOf() else graphService.getNeighborEdgeEntitySets(srcDstEntitySets, entityKeyIds)

        // ensure entity set access
        (neighborEdgeEntitySets + entitySetIds).filter { !authorizationManager.checkIfHasPermissions(AclKey(it), principals, EnumSet.of(Permission.READ)) }.let {
            if (it.isNotEmpty()) {
                throw ForbiddenException("Unable to perform delete on $entitySetIds because entity sets{$it} are inaccessible")
            }
        }

        // ensure access on property types
        val entitySetPropertyTypes = entitySetManager.getPropertyTypesOfEntitySets(neighborEdgeEntitySets + entitySetIds)

        val requiredPermissions = PERMISSIONS_FOR_DELETE_TYPE.getValue(deleteType)

        val unauthorized = mutableSetOf<AclKey>()
        authorizationManager.accessChecksForPrincipals(entitySetPropertyTypes.flatMap { (entitySetId, propertyTypes) ->
            propertyTypes.keys.map { ptId ->
                AccessCheck(AclKey(entitySetId, ptId), requiredPermissions)
            }
        }.toSet(), principals).forEach { authorization ->
            requiredPermissions.forEach { permission ->
                if (!authorization.permissions.getValue(permission)) {
                    unauthorized.add(authorization.aclKey)
                }
            }
        }

        if (unauthorized.isNotEmpty()) {
            logger.error("unable to perform $deleteType delete on $entitySetIds because $requiredPermissions permissions are required on $unauthorized")
            throw ForbiddenException("Unable to perform delete on  $entitySetIds because $requiredPermissions permissions are required on entity sets, neighbor edge entity sets and properties")
        }
    }

    override fun clearOrDeleteEntitiesAndNeighbors(
            entitySetIdEntityKeyIds: Map<UUID, Set<UUID>>,
            entitySetId: UUID,
            allEntitySetIds: Set<UUID>,
            filter: EntityNeighborsFilter,
            deleteType: DeleteType): UUID {

        return jobService.submitJob(DataDeletionJob(DataDeletionJobState(
                entitySetId,
                deleteType,
                filter.entityKeyIds,
                neighborDstEntitySetIds = filter.dstEntitySetIds.orElse(setOf()),
                neighborSrcEntitySetIds = filter.srcEntitySetIds.orElse(setOf())
        )))
    }
}
