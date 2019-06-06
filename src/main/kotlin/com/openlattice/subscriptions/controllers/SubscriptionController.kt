
package com.openlattice.subscriptions.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.authorization.Principals
import com.openlattice.graph.NeighborhoodQuery
import com.openlattice.subscriptions.SubscriptionApi
import com.openlattice.subscriptions.SubscriptionService
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.inject.Inject

@RestController
@RequestMapping(SubscriptionApi.CONTROLLER)
class SubscriptionController
@Inject
constructor(
        private val authorizationManager: AuthorizationManager,
        private val subscriptionService: SubscriptionService
) : SubscriptionApi, AuthorizingComponent {

    @Timed
    @RequestMapping(path = ["", "/"], method = [RequestMethod.PUT])
    override fun addSubscription(@RequestBody subscription: NeighborhoodQuery) {
        subscriptionService.addSubscription( subscription, Principals.getCurrentUser())
    }

    @Timed
    @RequestMapping(path = [SubscriptionApi.ENTITY_KEY_ID_PATH], method = [RequestMethod.POST])
    override fun updateSubscription( @RequestBody subscription: NeighborhoodQuery) {
        subscriptionService.updateSubscription( subscription, Principals.getCurrentUser())
    }

    @Timed
    @RequestMapping(path = [SubscriptionApi.ENTITY_KEY_ID_PATH], method = [RequestMethod.DELETE])
    override fun deleteSubscription(@PathVariable(SubscriptionApi.ENTITY_KEY_ID) subscriptionId: UUID) {
        subscriptionService.deleteSubscription( subscriptionId, Principals.getCurrentUser())
    }

    @Timed
    @RequestMapping(path = [SubscriptionApi.ALL], method = [RequestMethod.GET])
    override fun getAllSubscriptions(): Iterable<NeighborhoodQuery> {
        return subscriptionService.getAllSubscriptions(Principals.getCurrentUser())
    }

    @Timed
    @RequestMapping(path = ["", "/"], method = [RequestMethod.GET])
    override fun getSubscriptions(
            @RequestParam(SubscriptionApi.ENTITY_KEY_IDS) entityKeyIds: List<UUID>): Iterable<NeighborhoodQuery> {
        return subscriptionService.getSubscriptions(entityKeyIds, Principals.getCurrentUser())
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }
}

