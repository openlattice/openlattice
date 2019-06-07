package com.openlattice.subscriptions

import com.openlattice.graph.NeighborhoodQuery

data class SubscriptionContact (
        val subscription: NeighborhoodQuery,
        val type: String,
        val contact: String
)
