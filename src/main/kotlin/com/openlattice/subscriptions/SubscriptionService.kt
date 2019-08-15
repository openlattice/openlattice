package com.openlattice.subscriptions

import com.openlattice.authorization.Principal
import java.util.*

/**
 * This service is for managing Subscriptions on entities
 */
interface SubscriptionService {

    fun createOrUpdateSubscription(subscription: Subscription, user: Principal)

    fun deleteSubscription(ekId: UUID, user: Principal)

    fun getAllSubscriptions(user: Principal): Iterable<Subscription>

    fun getSubscriptions(ekIds: List<UUID>, user: Principal): Iterable<Subscription>

    fun getAllSubscriptions(): Iterable<Pair<Principal, Subscription>>

    fun markLastNotified(ekIds: Set<UUID>, user: Principal)

}