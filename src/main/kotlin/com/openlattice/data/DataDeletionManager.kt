package com.openlattice.data

import com.openlattice.authorization.Principal
import com.openlattice.search.requests.EntityNeighborsFilter
import java.util.*

interface DataDeletionManager {

    /**
     * Clears or deletes all entities from an entity set, as well as any edges and association entities, if authorized
     */
    fun clearOrDeleteEntitySet(
            entitySetId: UUID,
            deleteType: DeleteType,
            principals: Set<Principal>
    ): WriteEvent

    /**
     * Clears or deletes the specified entity key ids from an entity set, as well as any edges and association entities, if authorized
     */
    fun clearOrDeleteEntities(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            deleteType: DeleteType,
            principals: Set<Principal>
    ): WriteEvent

    /**
     * Clears or deletes the specified entity key ids from an entity set, as well as any edges filtered based on the
     * filter parameters, and the association entities and neighbor entities present on those edges, if authorized
     */
    fun clearOrDeleteEntitiesAndNeighborhood(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            srcEntitySetFilter: Set<UUID>,
            dstEntitySetFilter: Set<UUID>,
            deleteType: DeleteType,
            principals: Set<Principal>
    ): WriteEvent

    /**
     * Clears or deletes certain property values from the specified entity key ids from an entity set, if authorized
     */
    fun clearOrDeleteEntityProperties(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            deleteType: DeleteType,
            propertyTypeIds: Set<UUID>,
            principals: Set<Principal>
    ): WriteEvent



    /* The functions below do not perform auth checks and should only be used internally. */


    /**
     * Clears or deletes the specified entity key ids from an entity set, as well as any edges and association entities.
     *
     * This function does not perform authorization checks.
     */
    fun clearOrDeleteEntities(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            deleteType: DeleteType
    ): WriteEvent

    /**
     * Clears or deletes all entities from an entity set, as well as any edges and association entities.
     *
     * This function does not perform authorization checks.
     */
    fun clearOrDeleteEntitySet(
            entitySetId: UUID,
            deleteType: DeleteType
    ): WriteEvent

}