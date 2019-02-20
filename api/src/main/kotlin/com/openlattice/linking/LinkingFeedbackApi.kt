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

package com.openlattice.linking

import com.openlattice.data.EntityDataKey
import retrofit2.http.*

/**
 * This API is for managing manual feedbacks on linked people
 */
interface LinkingFeedbackApi {
    companion object {
        const val SERVICE = "/linker"
        const val CONTROLLER = "/linkingfeedback"
        const val BASE = SERVICE + CONTROLLER

        const val ENTITY = "/entity"
        const val FEATURES = "/features"
        const val ALL = "/all"

        const val FEEDBACK_TYPE = "feedbackType"
    }

    /**
     * Submits feedbacks for a given linking entity set and linking id in a batch format.
     */
    @PUT(BASE)
    fun addLinkingFeedback(@Body feedback: LinkingFeedback): Int

    /**
     * Returns positive/negative/all linking feedbacks on the given entity.
     */
    @POST(BASE + ENTITY)
    fun getLinkingFeedbacksOnEntity(@Query(FEEDBACK_TYPE) feedbackType: FeedbackType, @Body entity: EntityDataKey
    ): Iterable<EntityLinkingFeedback>

    /**
     * Returns the feedback on the given entity pair along with their features
     */
    @POST(BASE + FEATURES)
    fun getLinkingFeedbackWithFeatures(@Body entityPair: EntityKeyPair): EntityLinkingFeatures?

    /**
     * Returns all feedbacks submitted
     */
    @GET(BASE + ALL)
    fun getAllLinkingFeedbacks(): Iterable<EntityLinkingFeedback>

    /**
     * Returns all feedbacks submitted along with the features of pairwise entities
     */
    @GET(BASE + FEATURES + ALL)
    fun getAllLinkingFeedbacksWithFeatures(): Iterable<EntityLinkingFeatures>


    @HTTP(method = "DELETE", path = BASE, hasBody = true)
    fun deleteLinkingFeedback(@Body entityPair: EntityKeyPair): Int
}