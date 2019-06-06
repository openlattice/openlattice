
package com.openlattice.subscriptions.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.graph.NeighborhoodQuery
import com.openlattice.subscriptions.Subscription
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
    @RequestMapping(path = [SubscriptionApi.BASE], method = [RequestMethod.PUT])
    override fun addSubscription(@RequestBody subscription: NeighborhoodQuery): UUID {
        return subscriptionService.addSubscription( subscription )
    }

    @Timed
    @RequestMapping(path = [SubscriptionApi.BASE + SubscriptionApi.SUB_ID_PATH], method = [RequestMethod.POST])
    override fun updateSubscription(@PathVariable(SubscriptionApi.SUB_ID) subscriptionId: UUID,
                                    @RequestBody subscription: NeighborhoodQuery): UUID {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Timed
    @RequestMapping(path = [SubscriptionApi.BASE + SubscriptionApi.SUB_ID_PATH], method = [RequestMethod.DELETE])
    override fun deleteSubscription(@PathVariable(SubscriptionApi.SUB_ID) subscriptionId: UUID) {
        subscriptionService.deleteSubscription( subscriptionId )
    }

    @Timed
    @RequestMapping(path = [SubscriptionApi.BASE], method = [RequestMethod.GET])
    override fun getAllSubscriptions(): Iterable<Subscription> {
        return subscriptionService.getAllSubscriptions()
    }

    @Timed
    @RequestMapping(path = [SubscriptionApi.BASE], method = [RequestMethod.GET])
    override fun getSubscriptions(
            @RequestParam(SubscriptionApi.SUB_IDS) subscriptionIds: List<UUID>): Iterable<Subscription> {
        return subscriptionService.getSubscriptions(subscriptionIds)
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }
}

