package com.openlattice.subscriptions

import com.openlattice.graph.NeighborhoodQuery
import java.time.OffsetDateTime
import java.util.*

enum class SubscriptionContactType {
    EMAIL,
    PHONE
}

data class Subscription(
        val query: NeighborhoodQuery,
        val contact: Map<SubscriptionContactType, String>,
        val organizationId: UUID,
        val lastNotify: OffsetDateTime = OffsetDateTime.now().minusYears(100)
)
