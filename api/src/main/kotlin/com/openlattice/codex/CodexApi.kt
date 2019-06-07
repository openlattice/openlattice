package com.openlattice.codex

import com.twilio.rest.api.v2010.account.Message
import retrofit2.http.Body
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

//    @POST( BASE + ORG_ID_PATH )
    fun receiveIncomingText( @Body message: Message )

    @POST(BASE + ORG_ID_PATH )
    fun sendOutgoingText(@Path( ORG_ID ) organizationId: UUID, @Body contents: MessageRequest)

    @POST(BASE + STATUS )
    fun listenForTextStatus( @Body message: Message)

}
