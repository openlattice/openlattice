package com.openlattice.subscriptions

import com.openlattice.authorization.Principal
import com.openlattice.graph.NeighborhoodQuery
import java.util.*

/**
 * This service is for managing Subscriptions on entities
 */
interface SubscriptionService {

    fun createOrUpdateSubscription( subscription: NeighborhoodQuery, organizationId:UUID, user: Principal )

    fun deleteSubscription( ekId: UUID, user: Principal )

    fun getAllSubscriptions( user: Principal): Iterable<SubscriptionContact>

    fun getSubscriptions( ekIds: List<UUID>, user: Principal ): Iterable<SubscriptionContact>

    fun getAllSubscriptions():Iterable<Pair<Principal,SubscriptionContact>>

    fun createOrUpdateSubscriptionContact(contactInfo: SubscriptionContact, user: Principal)

    fun markLastNotified(ekIds: Set<UUID>, user: Principal )

}