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

        const val ALL = "/all"
        const val ENTITY_KEY_ID = "entityKeyId"
        const val ENTITY_KEY_ID_PATH = "/{$ENTITY_KEY_ID}"

        const val CONTACT = "contact"
        const val CONTACT_INFO_PATH = "/{$CONTACT}"
        const val ORGANIZATION = "organization"
        const val ORGANIZATION_PATH = "/$ORGANIZATION"
        const val ORGANIZATIION_ID_PATH = "/{$ORGANIZATION}"
    }

    /**
     * Creates a subscription
     */
    @POST(BASE + ORGANIZATION_PATH + ORGANIZATIION_ID_PATH)
    fun createOrUpdateSubscription(@Path(ORGANIZATION) organizationId: UUID, @Body subscription: NeighborhoodQuery)

    /**
     * Removes a subscription
     */
    @DELETE(BASE + ENTITY_KEY_ID_PATH)
    fun deleteSubscription(@Path(ENTITY_KEY_ID) subscriptionId: UUID)

    /**
     * Create or Update Subscription contact info
     */
    @POST(BASE + CONTACT_INFO_PATH)
    fun createOrUpdateSubscriptionContactInfo(@Body contactInfo: SubscriptionContact)

    /**
     * Returns all subscriptions
     */
    @GET(BASE + ALL)
    fun getAllSubscriptions(): Iterable<NeighborhoodQuery>

    /**
     * Returns subscription information for provided subscription ids
     */
    @GET(BASE)
    fun getSubscriptions(@Body entityKeyIds: List<UUID>): Iterable<NeighborhoodQuery>

}