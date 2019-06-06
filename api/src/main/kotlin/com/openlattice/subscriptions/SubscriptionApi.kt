/*
 * Copyright (C) 2019. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.subscriptions

import com.openlattice.graph.NeighborhoodQuery
import retrofit2.http.*
import java.util.*

/**
 * This API is for managing Subscriptions on entities
 */
interface SubscriptionApi {
    companion object {
        const val SERVICE = "/datastore"
        const val CONTROLLER = "/subscriptions"
        const val BASE = SERVICE + CONTROLLER

        const val SUB_ID = "subscriptionId"
        const val SUB_ID_PATH = "/{$SUB_ID}"

        const val SUB_IDS = "subscriptionIds"
    }

    /**
     * Creates a subscription
     */
    @PUT(BASE)
    fun addSubscription(@Body subscription: NeighborhoodQuery): UUID

    /**
     * Updates a subscription
     */
    @POST(BASE + SUB_ID_PATH)
    fun updateSubscription(@Path(SUB_ID) subscriptionId: UUID,
                           @Body subscription: NeighborhoodQuery): UUID

    /**
     * Removes a subscription
     */
    @DELETE(BASE + SUB_ID_PATH)
    fun deleteSubscription(@Path(SUB_ID) subscriptionId: UUID)

    /**
     * Returns all subscriptions
     */
    @GET(BASE)
    fun getAllSubscriptions(): Iterable<Subscription>

    /**
     * Returns subscription information for provided subscription ids
     */
    @GET(BASE)
    fun getSubscriptions(@Query(SUB_IDS) subscriptionIds: List<UUID>): Iterable<Subscription>

}