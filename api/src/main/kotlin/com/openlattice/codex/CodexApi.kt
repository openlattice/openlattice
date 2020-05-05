package com.openlattice.codex

import retrofit2.http.*
import java.util.*

interface CodexApi {
    companion object {
        const val SERVICE = "/datastore"
        const val CONTROLLER = "/codex"
        const val BASE = SERVICE + CONTROLLER

        const val ID = "id"
        const val ID_PATH = "/{$ID}"
        const val ORG_ID = "orgId"
        const val ORG_ID_PATH = "/{$ORG_ID}"
        const val PHONE = "phone"
        const val PHONE_PATH = "/{$PHONE}"

        const val INCOMING = "/incoming"
        const val MEDIA = "/media"
        const val STATUS = "/status"
        const val SCHEDULED = "/scheduled"
    }

    @POST(BASE + INCOMING + ORG_ID_PATH)
    fun receiveIncomingText(@Path(ORG_ID) organizationId: UUID)

    @POST(BASE)
    fun sendOutgoingText(@Body contents: MessageRequest)

    @POST(BASE + INCOMING + ORG_ID_PATH + STATUS)
    fun listenForTextStatus()

    @GET(BASE + MEDIA + ID_PATH)
    fun readAndDeleteMedia(@Path(ID) mediaId: UUID)

    @GET(BASE + SCHEDULED + ORG_ID_PATH)
    fun getUpcomingScheduledMessages(@Path(ORG_ID) organizationId: UUID): Map<UUID, MessageRequest>

    @GET(BASE + SCHEDULED + ORG_ID_PATH + PHONE_PATH)
    fun getUpcomingScheduledMessagesToPhoneNumber(@Path(ORG_ID) organizationId: UUID, @Path(PHONE) phone: String): Map<UUID, MessageRequest>

    @DELETE(BASE + SCHEDULED + ID_PATH)
    fun cancelScheduledMessage(@Path(ID) messageId: UUID)

}
