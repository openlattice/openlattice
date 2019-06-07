package com.openlattice.codex

import retrofit2.http.Body
import retrofit2.http.GET
import retrofit2.http.POST
import retrofit2.http.Path
import java.util.*

interface CodexApi {
    companion object{
        const val SERVICE = "/datastore"
        const val CONTROLLER = "/codex"
        const val BASE= SERVICE + CONTROLLER

        const val ORG_ID = "orgId"
        const val ORG_ID_PATH = "/{$ORG_ID}"
        const val STATUS = "/status"
    }

    fun receiveIncomingText()

    @POST(BASE + ORG_ID_PATH )
    fun sendOutgoingText(@Path( ORG_ID ) organizationId: UUID, @Body contents: MessageRequest)

    @GET(BASE + STATUS )
    fun listenForTextStatus()

}
