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
import com.google.common.collect.ListMultimap
import com.google.common.collect.SetMultimap
import com.openlattice.authorization.*
import com.openlattice.controllers.exceptions.ForbiddenException
import com.openlattice.data.requests.NeighborEntityDetails
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.*
import com.openlattice.graph.query.GraphQuery
import com.openlattice.graph.query.GraphQueryState
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
    override fun neighborhoodQuery(
            entitySetId: UUID,
            query: NeighborhoodQuery
    ): Map<UUID, List<NeighborEntityDetails>> {
        val entitySetsById = graphQueryService.getEntitySetForIds(query.ids)
        val (allEntitySetIds, requiredPropertyTypes) = getEntitySets(query, entitySetsById.values)

        /*
         * We need to figure out what property types are authorized for all entity sets
         */

        val authorizedPropertyTypes = edmAuthorizationHelper.getAuthorizedPropertiesOnEntitySets(
                allEntitySetIds,
                EnumSet.of(Permission.READ),
                Principals.getCurrentPrincipals()
        )

        ensureReadOnRequired(authorizedPropertyTypes, requiredPropertyTypes)


        graphQueryService.submitQuery(query)
        return mapOf()
    }


    @Timed
//    @PostMapping(
//            value = QUERY,
//            consumes = [MediaType.APPLICATION_JSON_VALUE],
//            produces = [MediaType.APPLICATION_JSON_VALUE]
//    )
    override fun submit(query: SimpleGraphQuery): GraphQueryState {
        //Collect the data to authorize

        //Collect the things to perserve
//        return graphQueryService.submitQuery(query);
        TODO("Not implemented")
    }

    @Timed
//    @PostMapping(
//            value = QUERY + ID_PATH,
//            consumes = [MediaType.APPLICATION_JSON_VALUE],
//            produces = [MediaType.APPLICATION_JSON_VALUE]
//    )
    override fun getQueryState(
            @PathVariable(ID) queryId: UUID,
            @RequestBody options: Set<GraphQueryState.Option>
    ): GraphQueryState {
        return graphQueryService.getQueryState(queryId, options)
    }

    override fun getQueryState(queryId: UUID): GraphQueryState {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Timed
    override fun getResults(queryId: UUID): SubGraph {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Timed
    override fun graphQuery(ops: GraphQuery): ListMultimap<UUID, SetMultimap<UUID, SetMultimap<UUID, Any>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }


    private fun getEntitySets(selection: NeighborhoodSelection): List<UUID> {
        return selection.outgingEntityTypeIds
                .map(edm::getEntitySetsOfType).orElseGet { emptyList() }
                .map(EntitySet::getId) +
                selection.outgingEntitySetIds.orElseGet { emptySet() } +
                selection.incomingEntityTypeIds
                        .map(edm::getEntitySetsOfType).orElseGet { emptyList() }
                        .map(EntitySet::getId) +
                selection.incomingEntitySetIds.orElseGet { emptySet() } +
                selection.associationTypeIds
                        .map(edm::getEntitySetsOfType).orElseGet { emptyList() }
                        .map(EntitySet::getId) +
                selection.associationEntitySetIds.orElseGet { emptySet() }
    }

    private fun getRequiredAuthorizations(selection: NeighborhoodSelection): Map<UUID, Set<UUID>> {
        return selection.outgoingEntityFilters.map { filters -> filters.mapValues { it.value.keys } }.orElseGet { emptyMap() } +
                selection.incomingEntityFilters.map { filters -> filters.mapValues { it.value.keys } }.orElseGet { emptyMap() } +
                selection.associationFilters.map { filters -> filters.mapValues { it.value.keys } }.orElseGet { emptyMap() }
    }

    private fun getEntitySets(
            query: NeighborhoodQuery, baseEntitySetIds: Collection<UUID>
    ): Pair<Set<UUID>, Map<UUID, Set<UUID>>> {
        val requiredAuthorizations: MutableMap<UUID, MutableSet<UUID>> = mutableMapOf()
        val allEntitySetIds = baseEntitySetIds +
                query.selections.flatMap {
                    getRequiredAuthorizations(it).forEach { entitySetId, requiredPropertyTypeIds ->
                        requiredAuthorizations
                                .getOrPut(entitySetId) { mutableSetOf() }
                                .addAll(requiredPropertyTypeIds)
                    }
                    getEntitySets(it)
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
