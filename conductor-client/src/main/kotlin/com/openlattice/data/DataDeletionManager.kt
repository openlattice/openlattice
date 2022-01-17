package com.openlattice.data

import com.openlattice.authorization.Principal
import com.geekbeast.controllers.exceptions.ForbiddenException
import com.openlattice.edm.type.PropertyType
import com.openlattice.search.requests.EntityNeighborsFilter
import java.util.*

interface DataDeletionManager {

    /**
     * Clears or deletes the specified entity key ids from an entity set, as well as any edges and association entities.
     *
     * This function does not perform authorization checks.
     */
    fun clearOrDeleteEntities(
            entitySetId: UUID,
            entityKeyIds: MutableSet<UUID>,
            deleteType: DeleteType
    ): UUID

    /**
     * Clears or deletes all entities from an entity set, as well as any edges and association entities.
     *
     * This function does not perform authorization checks.
     */
    fun clearOrDeleteEntitySet(
            entitySetId: UUID,
            deleteType: DeleteType
    ): UUID

    /**
     * Clears or deletes certain property values from the specified entity key ids from an entity set
     */
    fun clearOrDeleteEntityProperties(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            deleteType: DeleteType,
            propertyTypeIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent


    /**
     * Performs auth checks for [principals] in order to delete entities from entity set [entitySetId], throwing
     * a [ForbiddenException] if the user does not have required permissions for a delete of type [deleteType].
     */
    fun authCheckForEntitySetsAndNeighbors(
            entitySetIds: Set<UUID>,
            deleteType: DeleteType,
            principals: Set<Principal>,
            entityKeyIds: Set<UUID>? = null
    )

    /**
     * Clears or deletes the specified entity key ids and their neighboring entities, as well as any edges and associations
     * entities
     */
    fun clearOrDeleteEntitiesAndNeighbors(
            entitySetIdEntityKeyIds: Map<UUID, Set<UUID>>,
            entitySetId: UUID,
            allEntitySetIds: Set<UUID>,
            filter: EntityNeighborsFilter,
            deleteType: DeleteType
    ): UUID
}
