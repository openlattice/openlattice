package com.openlattice.codex.controllers

import com.codahale.metrics.annotation.Timed
import com.google.common.collect.Maps
import com.hazelcast.core.HazelcastInstance
import com.openlattice.apps.AppConfigKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.codex.CodexApi
import com.openlattice.codex.MessageRequest
import com.openlattice.controllers.exceptions.BadRequestException
import com.openlattice.data.DataApi
import com.openlattice.data.DataEdgeKey
import com.openlattice.datastore.apps.services.AppService
import com.openlattice.hazelcast.HazelcastQueue
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.postgres.mapstores.AppConfigMapstore
import com.openlattice.twilio.TwilioConfiguration
import com.twilio.Twilio
import com.twilio.rest.api.v2010.account.Message
import com.twilio.type.PhoneNumber
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RestController
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
        private val appService: AppService,
        private val appConfigMS: AppConfigMapstore,
        private val hazelcastInstance: HazelcastInstance,
        private val configuration: TwilioConfiguration
) : CodexApi, AuthorizingComponent {

    private val textingExecutor = Executors.newSingleThreadExecutor()
    private val twilioQueue = hazelcastInstance.getQueue<MessageRequest>(HazelcastQueue.TWILIO.name)
    val pendingTexts = Maps.newConcurrentMap<String, Message>()

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
    }

    init {
        Twilio.init( configuration.sid, configuration.token)

        textingExecutor.execute {
            Stream.generate { twilioQueue.take() }.forEach { (organizationId, messageContents, toPhoneNumber) ->
//                val phone = organizations.getOrganization(organizationId).phoneNumber
                val phone = "2403033965"
                if ( phone == "" ){
                    throw BadRequestException("No source phone number set for organization!")
                }
                val message = Message.creator(PhoneNumber(toPhoneNumber), PhoneNumber( phone ), messageContents)
                        .setStatusCallback(URI.create("https://api.openlattice.com/datastore/kodex/status")).create()
                pendingTexts.put(message.sid, message)
                processQueueEntry(message, organizationId )
            }
        }
    }

    @Timed
    @RequestMapping(path = ["", "/"], method = [RequestMethod.POST])
    override fun sendOutgoingText(@RequestBody contents: MessageRequest) {
        twilioQueue.put( contents )
    }

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

    fun createEntitiesFromMessage( msg: Message, organizationId: UUID, appTypeId: UUID ) {
        val entities = mutableListOf<Map<UUID, Set<Any>>>()
        val app = appService.getApp("codex")
        val ack = AppConfigKey(app.id, organizationId, appTypeId)
        val esid = appConfigMS.load(ack).entitySetId

        val createEntities = dataApi.createEntities( esid, entities )
    }

    fun createAssociationsFromMessage() {
        val deks = setOf<DataEdgeKey>()
        val createAssociations = dataApi.createAssociations(deks)
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }

}