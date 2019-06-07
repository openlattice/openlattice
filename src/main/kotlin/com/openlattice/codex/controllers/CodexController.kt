package com.openlattice.codex.controllers

import com.codahale.metrics.annotation.Timed
import com.google.common.collect.Maps
import com.google.common.collect.Queues
import com.openlattice.apps.AppApi
import com.openlattice.apps.AppConfigKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.codex.CodexApi
import com.openlattice.codex.MessageRequest
import com.openlattice.controllers.exceptions.BadRequestException
import com.openlattice.data.DataApi
import com.openlattice.data.DataEdgeKey
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.postgres.mapstores.AppConfigMapstore
import com.openlattice.twilio.TwilioConfiguration
import com.twilio.Twilio
import com.twilio.rest.api.v2010.account.Message
import com.twilio.rest.api.v2010.account.MessageCreator
import com.twilio.type.PhoneNumber
import org.springframework.web.bind.annotation.*
import java.net.URI
import java.util.*
import java.util.concurrent.Executors
import java.util.stream.Stream
import javax.inject.Inject

@RestController
@RequestMapping(CodexApi.CONTROLLER)
class CodexController
@Inject
constructor(
        private val authorizationManager: AuthorizationManager,
        private val organizations: HazelcastOrganizationService,
        private val dataApi: DataApi,
        private val appApi: AppApi,
        private val appConfigMS: AppConfigMapstore,
        private val configuration: TwilioConfiguration
) : CodexApi, AuthorizingComponent {

    // Codex App
    private val app = appApi.getApp("codex")

    companion object {
        // People
        private val peopleUUID = UUID.fromString("6e8e22d6-4b3d-43a1-b154-f4c1bcdfe55f")
        // Settings
        private val settingsUUID = UUID.fromString("4a88ceb3-917c-4454-a818-c626f177958f")
        // ContactInfo
        private val contactUUID = UUID.fromString("35e6a833-9c25-43ad-b116-e9dd11d08cfa")
        // Messages
        private val messagesUUID = UUID.fromString("55122a60-4858-4ba0-bb6f-93bd9f75749b")
        // TO
        private val toUUID = UUID.fromString("3ddfe8d0-0b73-4097-b737-e969b64b4f64")
        // From
        private val fromUUID = UUID.fromString("2d2509ea-51f7-4663-a3a8-42d8036e2e04")

        private val textingExecutor = Executors.newSingleThreadExecutor()

        val unSentTexts = Queues.newArrayBlockingQueue<MessageCreator>( 2 )
        val pendingTexts = Maps.newConcurrentMap<String, Message>()
    }

    init {
        Twilio.init( configuration.sid, configuration.token)

        textingExecutor.execute {
            Stream.generate { unSentTexts.take() }.map {
                val message = it.create()
                pendingTexts.put(message.sid, message)
            }
        }
    }

    @Timed
    @RequestMapping(path = [CodexApi.ORG_ID_PATH], method = [RequestMethod.POST])
    override fun sendOutgoingText(@PathVariable(CodexApi.ORG_ID) organizationId: UUID, @RequestBody contents: MessageRequest) {
        val organization = organizations.getOrganization(organizationId)
        val phoneNumber = organization.phoneNumber
//        val phoneNumber = "2403033965"
        if ( organization.phoneNumber == "" ){
             throw BadRequestException("No phone number set for organization!")
        }

        val statusCallback = Message.creator(PhoneNumber(contents.phoneNumber), PhoneNumber( phoneNumber ), contents.messageContents)
                .setStatusCallback(URI.create("https://api.openlattice.com/datastore/kodex/status"))
        unSentTexts.put(statusCallback)
    }

    @Timed
    @RequestMapping(path = [CodexApi.STATUS], method = [RequestMethod.POST])
    override fun receiveIncomingText( @RequestBody message: Message  ) {

    }

    @Timed
    @RequestMapping(path = [CodexApi.STATUS], method = [RequestMethod.POST])
    override fun listenForTextStatus( @RequestBody message: Message ) {
        if ( message.status == Message.Status.FAILED || message.status == Message.Status.UNDELIVERED ){
            println( "Message not received or even failed to send!!! ")
        } else {
            pendingTexts.remove( message.sid )
        }
    }

    fun processQueueEntry(message: Message, organizationId: UUID) {
        createEntitiesFromMessage( message, organizationId, peopleUUID)
        createEntitiesFromMessage( message, organizationId, contactUUID)
        createEntitiesFromMessage( message, organizationId, messagesUUID)
        createEntitiesFromMessage( message, organizationId, toUUID)
        createEntitiesFromMessage( message, organizationId, fromUUID)

        createAssociationsFromMessage()
    }

    fun createEntitiesFromMessage( msg: Message, organizationId: UUID, appTypeId: UUID ) : List<Map<UUID, Set<Any>>> {
        val entities = mutableListOf<Map<UUID, Set<Any>>>()

        val ack = AppConfigKey(app.id, organizationId, appTypeId)

        val esid = appConfigMS.load(ack).entitySetId

        val createEntities = dataApi.createEntities(
                esid,
                listOf(
                        mapOf<UUID, Set<Any>>( Pair( UUID.randomUUID() , setOf<Any>()) )
                )
        )

        return listOf()
    }

    fun createAssociationsFromMessage() {
        val deks = setOf<DataEdgeKey>()
        val createAssociations = dataApi.createAssociations(deks)
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }

}