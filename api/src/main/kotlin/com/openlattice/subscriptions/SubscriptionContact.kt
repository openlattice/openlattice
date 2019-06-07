package com.openlattice.subscriptions

import com.openlattice.graph.NeighborhoodQuery
import java.time.OffsetDateTime

enum class SubscriptionContactType {
    EMAIL,
    PHONE
}

data class SubscriptionContact (
        val subscription: NeighborhoodQuery,
        val contact : Map<SubscriptionContactType, String>,
        val lastNotify : OffsetDateTime = OffsetDateTime.now().minusYears(100)
)
