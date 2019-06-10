package com.openlattice.subscriptions.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.analysis.requests.Filter
import com.openlattice.analysis.requests.WrittenTwoWeeksFilter
import com.openlattice.authorization.*
import com.openlattice.datastore.services.EdmService
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.GraphQueryService
import com.openlattice.graph.Neighborhood
import com.openlattice.graph.NeighborhoodQuery
import com.openlattice.graph.NeighborhoodSelection
import com.openlattice.subscriptions.FeedsApi
import com.openlattice.subscriptions.SubscriptionService
import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RestController
import java.util.*
import javax.inject.Inject

@RestController
@RequestMapping(FeedsApi.CONTROLLER)
class FeedsController
@Inject
constructor(
        private val authorizationManager: AuthorizationManager,
        private val subscriptionService: SubscriptionService,
        private val graphQueryService: GraphQueryService,
        private val edmService: EdmService,
        private val edmAuthorizationHelper: EdmAuthorizationHelper
) : FeedsApi, AuthorizingComponent {
    companion object {
        private val logger = LoggerFactory.getLogger(FeedsController::class.java)!!
    }

    @Timed
    @RequestMapping(path = ["", "/"], method = [RequestMethod.GET])
    override fun getLatestFeed(): Iterator<Neighborhood> {
        return subscriptionService.getAllSubscriptions(Principals.getCurrentUser()).map { subscriptionContact ->
            val query = subscriptionContact.subscription
            val entitySetsById = graphQueryService.getEntitySetForIds(query.ids)
            val (allEntitySetIds, _) = resolveEntitySetIdsAndRequiredAuthorizations(
                    query,
                    entitySetsById.values
            )

            val authorizedPropertyTypes = edmAuthorizationHelper.getAuthorizedPropertiesOnEntitySets(
                    allEntitySetIds,
                    EnumSet.of(Permission.READ),
                    Principals.getCurrentPrincipals()
            )

            val propertyTypes = authorizedPropertyTypes.values.flatMap { it.values }.associateBy { it.id }

            val submitQuery = graphQueryService.submitQuery(
                    query, propertyTypes, authorizedPropertyTypes, Optional.of(WrittenTwoWeeksFilter())
            )

            subscriptionService.markLastNotified(query.ids, Principals.getCurrentUser());
            return@map submitQuery
        }.listIterator()
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

    private fun getRequiredAuthorizations(selection: NeighborhoodSelection): Map<UUID, Set<UUID>> {
        return selection.entityFilters.map { filters -> filters.mapValues { it.value.keys } }.orElseGet { emptyMap() } +
                selection.associationFilters.map { filters -> filters.mapValues { it.value.keys } }.orElseGet { emptyMap() }
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }

    fun insertRecencyFilter(
            filtersMap: Optional<Map<UUID, Map<UUID, Set<Filter>>>>
    ): Optional<Map<UUID, Map<UUID, Set<Filter>>>> {
        if (filtersMap.isEmpty) {
            return Optional.empty()
        }
//        val newEntityFilterMap =  filtersMap.get().mapValues {
//            val newFilterMap = mutableMapOf<UUID, Set<Filter>>()
//            it.value.forEach {
//                val newFilterSet = setOf(WrittenTwoWeeksFilter()).plus(it.value)
//                newFilterMap.put(it.key, newFilterSet)
//            }
//            return@mapValues newFilterMap
//        }

        val newEntityFilterMap = mutableMapOf<UUID, Map<UUID, Set<Filter>>>()
        filtersMap.get().forEach {
            val newFilterMap = mutableMapOf<UUID, Set<Filter>>()
            it.value.forEach {
                val newFilterSet = it.value.plus(WrittenTwoWeeksFilter())
                newFilterMap.put(it.key, newFilterSet)
            }
            newEntityFilterMap.put(it.key, newFilterMap)
        }
        return Optional.of(newEntityFilterMap)
    }

    fun rebuildSelectionList(targetList: List<NeighborhoodSelection>): List<NeighborhoodSelection> {
        val newList = mutableListOf<NeighborhoodSelection>()
        targetList.forEachIndexed { index, neighborhoodSelection ->
            var newEntityFilterMap = insertRecencyFilter(neighborhoodSelection.entityFilters)
            if (newEntityFilterMap.isEmpty && neighborhoodSelection.entityTypeIds.isPresent) {
                newEntityFilterMap = buildMapWithRecencyFilterFromTypeIds(neighborhoodSelection.entityTypeIds.get())
            } else if (newEntityFilterMap.isEmpty && neighborhoodSelection.entitySetIds.isPresent) {
                newEntityFilterMap = buildMapWithRecencyFilterFromESIds(neighborhoodSelection.entitySetIds.get())
            } else {
                newEntityFilterMap = Optional.empty()
            }

            var newAssociationFilterMap = insertRecencyFilter(neighborhoodSelection.associationFilters)
            if (newAssociationFilterMap.isEmpty && neighborhoodSelection.associationTypeIds.isPresent) {
                newAssociationFilterMap = buildMapWithRecencyFilterFromTypeIds(
                        neighborhoodSelection.associationTypeIds.get()
                )
            } else if (newAssociationFilterMap.isEmpty && neighborhoodSelection.associationEntitySetIds.isPresent) {
                newAssociationFilterMap = buildMapWithRecencyFilterFromESIds(
                        neighborhoodSelection.associationEntitySetIds.get()
                )
            } else {
                newAssociationFilterMap = Optional.empty()
            }

            newList.add(
                    NeighborhoodSelection(
                            neighborhoodSelection.entityTypeIds,
                            neighborhoodSelection.entitySetIds,
                            newEntityFilterMap,
                            neighborhoodSelection.associationTypeIds,
                            neighborhoodSelection.associationEntitySetIds,
                            newAssociationFilterMap
                    )
            )
        }
        return newList
    }


    private fun buildMapWithRecencyFilterFromIdSet(
            ids: Set<UUID>, typeGenFunction: (id: UUID) -> Map<UUID, PropertyType>
    ): Optional<Map<UUID, Map<UUID, Set<Filter>>>> {
        val etidMap = mutableMapOf<UUID, Map<UUID, Set<Filter>>>()
        ids.forEach {
            val typeMap = mutableMapOf<UUID, Set<Filter>>()
            val propertyTypesForEntitySet = typeGenFunction(it)
            propertyTypesForEntitySet.keys.forEach {
                typeMap.put(it, setOf(WrittenTwoWeeksFilter()))
            }
            etidMap.put(it, typeMap)
        }
        return Optional.of(etidMap)
    }

    private fun buildMapWithRecencyFilterFromESIds(esIds: Set<UUID>): Optional<Map<UUID, Map<UUID, Set<Filter>>>> {
        return buildMapWithRecencyFilterFromIdSet(esIds) { edmService.getPropertyTypesForEntitySet(it) }
    }

    private fun buildMapWithRecencyFilterFromTypeIds(typeIds: Set<UUID>): Optional<Map<UUID, Map<UUID, Set<Filter>>>> {
        return buildMapWithRecencyFilterFromIdSet(typeIds) { edmService.getPropertyTypesOfEntityType(it) }
    }

}

