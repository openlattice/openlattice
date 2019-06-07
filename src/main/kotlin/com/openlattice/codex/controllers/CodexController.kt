package com.openlattice.codex.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.codex.CodexApi
import com.openlattice.codex.MessageRequest
import com.openlattice.controllers.exceptions.BadRequestException
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.twilio.TwilioConfiguration
import com.twilio.Twilio
import com.twilio.rest.api.v2010.account.Message
import com.twilio.type.PhoneNumber
import org.springframework.web.bind.annotation.*
import java.net.URI
import java.util.*
import javax.inject.Inject

@RestController
@RequestMapping(CodexApi.CONTROLLER)
class CodexController
@Inject
constructor(
        private val authorizationManager: AuthorizationManager,
        private val organizations: HazelcastOrganizationService,
        private val configuration: TwilioConfiguration
) : CodexApi, AuthorizingComponent {

//    companion object {
//        val deliveryStatus = mutableMapOf<UUID, Pair<MessageRequest, Message>>()
//    }

    init {
        Twilio.init( configuration.sid, configuration.token)
    }

    override fun receiveIncomingText( @RequestBody message: Message  ) {

    }

    @Timed
    @RequestMapping(path = [CodexApi.ORG_ID_PATH], method = [RequestMethod.POST])
    override fun sendOutgoingText(@PathVariable(CodexApi.ORG_ID) organizationId: UUID, @RequestBody contents: MessageRequest) {
        val organization = organizations.getOrganization(organizationId)
        if ( organization.phoneNumber == "" ){
             throw BadRequestException("No phone number set for organization!")
        }

        Message.creator( PhoneNumber( contents.phoneNumber ), PhoneNumber( organization.phoneNumber ), contents.messageContents )
                .setStatusCallback( URI.create( "https://api.openlattice.com/datastore/kodex/status" ) )
                .create()
    }

    @Timed
    @RequestMapping(path = [CodexApi.STATUS], method = [RequestMethod.POST])
    override fun listenForTextStatus( @RequestBody message: Message ) {
        if ( message.status == Message.Status.FAILED || message.status == Message.Status.UNDELIVERED ){
            println( "Message not received or even failed to send!!! ")
        }
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }

}