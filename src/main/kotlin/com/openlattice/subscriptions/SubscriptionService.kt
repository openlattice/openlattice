package com.openlattice.subscriptions

import com.openlattice.graph.NeighborhoodQuery
import java.util.*

/**
 * This service is for managing Subscriptions on entities
 */
interface SubscriptionService {

    fun addSubscription( subscription: NeighborhoodQuery ): UUID

    fun updateSubscription( subscription: Subscription ): UUID

    fun deleteSubscription( subId: UUID )

    fun getAllSubscriptions(): Iterable<Subscription>

    fun getSubscriptions( subIds: List<UUID> ): Iterable<Subscription>

}