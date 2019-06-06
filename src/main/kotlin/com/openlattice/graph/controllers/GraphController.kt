/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.graph.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.authorization.*
import com.openlattice.controllers.exceptions.ForbiddenException
import com.openlattice.data.requests.NeighborEntityDetails
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.*
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.inject.Inject

/**
 *
 */
@RestController
@RequestMapping(CONTROLLER)
class GraphController
@Inject
constructor(
        private val graphQueryService: GraphQueryService,
        private val authorizationManager: AuthorizationManager,
        private val edm: EdmManager,
        private val edmAuthorizationHelper: EdmAuthorizationHelper
//        private val filteredAggregation
) : GraphApi, AuthorizingComponent {
    @Timed
    @PostMapping(
            value = [NEIGHBORS + ENTITY_SET_ID_PATH],
            consumes = [MediaType.APPLICATION_JSON_VALUE],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )

    override fun neighborhoodQuery(
            entitySetId: UUID,
            query: NeighborhoodQuery
    ): Map<UUID, List<NeighborEntityDetails>> {
        val entitySetsById = graphQueryService.getEntitySetForIds(query.ids)
        val (allEntitySetIds, requiredPropertyTypes) = resolveEntitySetIdsAndRequiredAuthorizations(
                query,
                entitySetsById.values
        )

        /*
         * We need to figure out what property types are authorized for all entity sets
         */

        val authorizedPropertyTypes = edmAuthorizationHelper.getAuthorizedPropertiesOnEntitySets(
                allEntitySetIds,
                EnumSet.of(Permission.READ),
                Principals.getCurrentPrincipals()
        )

        ensureReadOnRequired(authorizedPropertyTypes, requiredPropertyTypes)

        val propertyTypes = authorizedPropertyTypes.values.flatMap { it.values }.associateBy { it.id }
        graphQueryService.submitQuery(query, propertyTypes, authorizedPropertyTypes.mapValues { it.value.keys })
        return mapOf()
    }

    private fun getRequiredAuthorizations(selection: NeighborhoodSelection): Map<UUID, Set<UUID>> {
        return selection.entityFilters.map { filters -> filters.mapValues { it.value.keys } }.orElseGet { emptyMap() } +
                selection.associationFilters.map { filters -> filters.mapValues { it.value.keys } }.orElseGet { emptyMap() }
    }

    private fun resolveEntitySetIdsAndRequiredAuthorizations(
            query: NeighborhoodQuery, baseEntitySetIds: Collection<UUID>
    ): Pair<Set<UUID>, Map<UUID, Set<UUID>>> {
        val requiredAuthorizations: MutableMap<UUID, MutableSet<UUID>> = mutableMapOf()
        val allEntitySetIds = baseEntitySetIds.asSequence() +
                (query.srcSelections.asSequence() + query.dstSelections.asSequence()).flatMap { selection ->
                    getRequiredAuthorizations(selection).forEach { entitySetId, requiredPropertyTypeIds ->
                        requiredAuthorizations
                                .getOrPut(entitySetId) { mutableSetOf() }
                                .addAll(requiredPropertyTypeIds)
                    }
                    //
                    graphQueryService.getEntitySets(selection.entityTypeIds).asSequence() +
                            graphQueryService.getEntitySets(selection.associationTypeIds).asSequence()
                }

        return allEntitySetIds.toSet() to requiredAuthorizations
    }

    private fun ensureReadOnRequired(
            authorizedPropertyTypes: Map<UUID, MutableMap<UUID, PropertyType>>,
            requiredPropertyTypes: Map<UUID, Set<UUID>>
    ) {
        if (!requiredPropertyTypes.all { (entitySetId, propertyTypeIds) ->
                    authorizedPropertyTypes.containsKey(entitySetId) && authorizedPropertyTypes.getValue(
                            entitySetId
                    ).keys.containsAll(propertyTypeIds)
                }) {
            throw ForbiddenException("Not authorized to perform this operation!")
        }
    }


    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }
}
