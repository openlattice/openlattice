package com.openlattice.subscriptions

import com.openlattice.data.EntityKey
import java.util.*
import java.util.stream.Stream

interface EntityFeedService {

    fun getFeedDataForSubscription( subscriptionId: UUID): Stream<EntityKey>
}