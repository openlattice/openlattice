package com.openlattice.linking

import retrofit2.http.Body
import retrofit2.http.GET
import retrofit2.http.PUT

/**
 * This API is for managing manual feedbacks on linked people
 */
interface LinkingFeedbackApi {
    companion object {
        const val SERVICE = "/datastore"
        const val CONTROLLER = "/linking"
        const val BASE = SERVICE + CONTROLLER

        const val FEEDBACK = "/feedback"
        const val ALL = "/all"
        const val FEATURES = "/features"
    }

    @PUT(BASE + FEEDBACK)
    fun addLinkingFeedback(@Body feedback: EntityLinkingFeedback): Boolean

    @GET(BASE + FEEDBACK + ALL)
    fun getLinkingFeedbacks(): Iterable<EntityLinkingFeedback>

    @GET(BASE + FEEDBACK + ALL + FEATURES)
    fun getLinkingFeedbacksWithFeatures(): Iterable<EntityLinkingFeatures>
}