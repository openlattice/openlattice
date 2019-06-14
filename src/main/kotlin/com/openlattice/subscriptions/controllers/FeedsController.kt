package com.openlattice.subscriptions.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.analysis.requests.WrittenTwoWeeksFilter
import com.openlattice.authorization.*
import com.openlattice.graph.GraphQueryService
import com.openlattice.graph.Neighborhood
import com.openlattice.graph.NeighborhoodQuery
import com.openlattice.graph.NeighborhoodSelection
import com.openlattice.subscriptions.FeedsApi
import com.openlattice.subscriptions.LastWriteRangeFilter
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
        private val edmAuthorizationHelper: EdmAuthorizationHelper
) : FeedsApi, AuthorizingComponent {
    companion object {
        private val logger = LoggerFactory.getLogger(FeedsController::class.java)!!
    }

    @Timed
    @RequestMapping(path = ["", "/"], method = [RequestMethod.GET])
    override fun getLatestFeed(): Iterator<Neighborhood> {
        return subscriptionService.getAllSubscriptions(Principals.getCurrentUser()).map { subscriptionContact ->
            val query = subscriptionContact.query
            val ids = query.ids.values.flatMap { it.orElse(emptySet()) }.toSet()
            val entitySetsById = graphQueryService.getEntitySetForIds(ids)


            entitySetsById.asSequence()
                    .groupBy({ it.value }, { it.key })
                    .mapValues { it.value.toSet() }
                    .forEach { (entitySetId, ids) ->
                        check(query.ids.containsKey(entitySetId)) {
                            "Entity set id ($entitySetId) / entity key ids ($ids) mismatch."
                        }
                        val maybeIds = query.ids.getValue(entitySetId)
                        check(maybeIds.isPresent) {
                            "Entity set id ($entitySetId) expected to have entity key ids ($ids), instead found none."
                        }

                        val missing = maybeIds.get() - ids
                        val additional = ids - maybeIds.get()

                        check(missing.isEmpty() && additional.isEmpty()) {
                            "Missing keys ($missing) and additional keys ($additional) are incorrectly specified."
                        }
                    }

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
                    query, propertyTypes, authorizedPropertyTypes,
                    Optional.of(LastWriteRangeFilter(subscriptionContact.lastNotify))
            )

            subscriptionService.markLastNotified(ids, Principals.getCurrentUser())
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

}

