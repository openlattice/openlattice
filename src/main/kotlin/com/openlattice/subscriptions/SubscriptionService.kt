package com.openlattice.subscriptions

import com.openlattice.graph.NeighborhoodQuery
import java.util.*
import java.util.stream.Stream

interface SubscriptionService {

    fun addSubscription( subscription: NeighborhoodQuery )

    fun updateSubscription( subscription: Subscription )

    fun deleteSubscription( subId: UUID )

    fun getAllSubscriptions(): Stream<Subscription>

    fun getSubscription( subIds: List<UUID> ): Subscription

}