package com.openlattice.subscriptions.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.analysis.requests.Filter
import com.openlattice.analysis.requests.WrittenTwoWeeksFilter
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.authorization.EdmAuthorizationHelper
import com.openlattice.authorization.Principals
import com.openlattice.graph.*
import com.openlattice.subscriptions.FeedsApi
import com.openlattice.subscriptions.SubscriptionService
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
        private val authzHelper: EdmAuthorizationHelper,
        private val subscriptionService: SubscriptionService,
        private val graphApi: GraphApi,
        private val graphQueryService: GraphQueryService
) : FeedsApi, AuthorizingComponent {

    @Timed
    @RequestMapping(path = ["", "/"], method = [RequestMethod.GET])
    override fun getLatestFeed(): Iterator<Neighborhood> {
        return subscriptionService.getAllSubscriptions(Principals.getCurrentUser()).map { neighborhoodQuery ->
            val newSrcList = rebuildSelectionList( neighborhoodQuery.srcSelections )
            val newDstList = rebuildSelectionList( neighborhoodQuery.dstSelections )
            graphApi.neighborhoodQuery(
                    UUID.randomUUID(),
                    NeighborhoodQuery(neighborhoodQuery.ids, newSrcList, newDstList))
        }.listIterator()
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }

    fun insertRecencyFilter( filtersMap: Optional<Map<UUID, Map<UUID, Set<Filter>>>> ) : Optional<Map<UUID, Map<UUID, Set<Filter>>>> {
        val newEntityFilterMap = mutableMapOf<UUID, Map<UUID, Set<Filter>>>()
        filtersMap.get().forEach {
            val newFilterMap = mutableMapOf<UUID, Set<Filter>>()
            it.value.forEach {
                val newFilterSet = it.value.plus(WrittenTwoWeeksFilter())
                newFilterMap.put(it.key, newFilterSet)
            }
            newEntityFilterMap.put( it.key, newFilterMap )
        }
        return Optional.of(newEntityFilterMap)
    }

    fun rebuildSelectionList( targetList: List<NeighborhoodSelection>) : List<NeighborhoodSelection> {
        val newList = mutableListOf<NeighborhoodSelection>()
        targetList.forEachIndexed { index, neighborhoodSelection ->
            val newEntityFilterMap = insertRecencyFilter(neighborhoodSelection.entityFilters)
            val newAssociationFilterMap = insertRecencyFilter(neighborhoodSelection.associationFilters)

            newList.add( NeighborhoodSelection(
                    neighborhoodSelection.entityTypeIds,
                    neighborhoodSelection.entitySetIds,
                    newEntityFilterMap,
                    neighborhoodSelection.associationTypeIds,
                    neighborhoodSelection.associationEntitySetIds,
                    newAssociationFilterMap))
        }
        return newList
    }

}

