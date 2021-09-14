package com.openlattice.subscriptions.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.authorization.*
import com.openlattice.controllers.exceptions.BadRequestException
import com.openlattice.controllers.exceptions.ForbiddenException
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.GraphQueryService
import com.openlattice.graph.NeighborhoodQuery
import com.openlattice.graph.NeighborhoodSelection
import com.openlattice.subscriptions.Subscription
import com.openlattice.subscriptions.SubscriptionApi
import com.openlattice.subscriptions.SubscriptionService
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.inject.Inject

@SuppressFBWarnings(
        value = ["BC_BAD_CAST_TO_ABSTRACT_COLLECTION"],
        justification = "Allowing kotlin collection mapping cast to List")
@RestController
@RequestMapping(SubscriptionApi.CONTROLLER)
class SubscriptionController
@Inject
constructor(
        private val authorizationManager: AuthorizationManager,
        private val graphQueryService: GraphQueryService,
        private val subscriptionService: SubscriptionService,
        private val edmAuthorizationHelper: EdmAuthorizationHelper
) : SubscriptionApi, AuthorizingComponent {

    @Timed
    @RequestMapping(path = ["", "/"], method = [RequestMethod.PUT])
    override fun createOrUpdateSubscriptionContactInfo(@RequestBody contactInfo: Subscription) {
        val query = contactInfo.query
        if (query.ids.isEmpty()) {
            throw BadRequestException("Must specify entity key ids to subscribe to")
        }

        val entitySetsById = graphQueryService.getEntitySetForIds(
                query.ids.values.flatMap { it.orElse(emptySet()) }.toSet()
        )

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

        val (allEntitySetIds, requiredPropertyTypes) = resolveEntitySetIdsAndRequiredAuthorizations(
                query,
                entitySetsById.values
        )

        val authorizedPropertyTypes = edmAuthorizationHelper.getAuthorizedPropertiesOnEntitySets(
                allEntitySetIds,
                EnumSet.of(Permission.READ),
                Principals.getCurrentPrincipals()
        )

        ensureReadOnRequired(authorizedPropertyTypes, requiredPropertyTypes)
        subscriptionService.createOrUpdateSubscription(contactInfo, Principals.getCurrentUser())
    }

    @Timed
    @RequestMapping(path = [SubscriptionApi.ENTITY_KEY_ID_PATH], method = [RequestMethod.DELETE])
    override fun deleteSubscription(@PathVariable(SubscriptionApi.ENTITY_KEY_ID) subscriptionId: UUID) {
        subscriptionService.deleteSubscription(subscriptionId, Principals.getCurrentUser())
    }

    @Timed
    @RequestMapping(path = ["", "/"], method = [RequestMethod.GET])
    override fun getAllSubscriptions(): Iterable<Subscription> {
        return subscriptionService.getAllSubscriptions(Principals.getCurrentUser())
    }

    @Timed
    @RequestMapping(path = ["", "/"], method = [RequestMethod.POST])
    override fun getSubscriptions(@RequestBody entityKeyIds: List<UUID>): Iterable<Subscription> {
        return subscriptionService.getSubscriptions(entityKeyIds, Principals.getCurrentUser())
    }

    private fun resolveEntitySetIdsAndRequiredAuthorizations(
            query: NeighborhoodQuery, baseEntitySetIds: Collection<UUID>
    ): Pair<Set<UUID>, Map<UUID, Set<UUID>>> {
        val requiredAuthorizations: MutableMap<UUID, MutableSet<UUID>> = mutableMapOf()
        val allEntitySetIds = baseEntitySetIds.asSequence() +
                (query.srcSelections.asSequence() + query.dstSelections.asSequence()).flatMap { selection ->
                    getRequiredAuthorizations(selection).forEach { (entitySetId, requiredPropertyTypeIds) ->
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

    private fun getRequiredAuthorizations(selection: NeighborhoodSelection): Map<UUID, Set<UUID>> {
        return selection.entityFilters.map { filters -> filters.mapValues { it.value.keys } }.orElseGet { emptyMap() } +
                selection.associationFilters.map { filters -> filters.mapValues { it.value.keys } }.orElseGet { emptyMap() }
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }
}

