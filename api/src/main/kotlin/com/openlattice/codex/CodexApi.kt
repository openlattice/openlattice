package com.openlattice.codex

import com.twilio.rest.api.v2010.account.Message
import retrofit2.http.Body
import retrofit2.http.POST
import retrofit2.http.Path
import java.util.*

interface CodexApi {
    companion object {
        const val SERVICE = "/datastore"
        const val CONTROLLER = "/codex"
        const val BASE = SERVICE + CONTROLLER

        const val ORG_ID = "orgId"
        const val ORG_ID_PATH = "/{$ORG_ID}"
        const val INCOMING = "/incoming"
        const val STATUS = "/status"
    }

    @POST(BASE + INCOMING + ORG_ID_PATH)
    fun receiveIncomingText(@Path(ORG_ID) organizationId: UUID)

    @POST(BASE)
    fun sendOutgoingText(@Body contents: MessageRequest)

    @POST(BASE + INCOMING + ORG_ID_PATH + STATUS)
    fun listenForTextStatus()

}
