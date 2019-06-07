package com.openlattice.subscriptions.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.analysis.requests.Filter
import com.openlattice.analysis.requests.WrittenTwoWeeksFilter
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.authorization.Principals
import com.openlattice.graph.GraphApi
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
        private val graphApi: GraphApi
) : FeedsApi, AuthorizingComponent {
    companion object {
        private val logger = LoggerFactory.getLogger(FeedsController::class.java)!!
    }

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
        if (filtersMap.isEmpty){
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

        val newEntityFilterMap =  mutableMapOf<UUID, Map<UUID, Set<Filter>>>()
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
            var newEntityFilterMap = insertRecencyFilter(neighborhoodSelection.entityFilters)
            if ( newEntityFilterMap.isEmpty ){
                newEntityFilterMap = buildMapWithRecencyFilter(Pair(neighborhoodSelection.entitySetIds.get(), neighborhoodSelection.entityTypeIds.get() ))
            }

            var newAssociationFilterMap = insertRecencyFilter(neighborhoodSelection.associationFilters)
            if ( newAssociationFilterMap.isEmpty ){
                newAssociationFilterMap = buildMapWithRecencyFilter( Pair(neighborhoodSelection.associationEntitySetIds.get(), neighborhoodSelection.associationTypeIds.get() ))
            }

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

    private fun buildMapWithRecencyFilter( setTypeIdsPair: Pair<Set<UUID>, Set<UUID>>): Optional<Map<UUID, Map<UUID, Set<Filter>>>> {
        val esidMap = mutableMapOf<UUID, Map<UUID, Set<Filter>>>()
        setTypeIdsPair.first.forEach { esid ->
            val typeMap = mutableMapOf<UUID, Set<Filter>>()
            setTypeIdsPair.second.forEach {etid ->
                typeMap.put(etid, setOf(WrittenTwoWeeksFilter()))
            }
            esidMap.put(esid, typeMap)
        }
        return Optional.of(esidMap)
    }

}

