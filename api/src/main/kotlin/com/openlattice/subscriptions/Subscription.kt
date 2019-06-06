package com.openlattice.subscriptions

import com.openlattice.graph.NeighborhoodQuery
import java.util.*

data class Subscription(
    val id: UUID,
    val query: NeighborhoodQuery
) {

}