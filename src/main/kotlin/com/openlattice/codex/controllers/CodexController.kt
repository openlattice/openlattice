package com.openlattice.codex.controllers

import com.codahale.metrics.annotation.Timed
import com.google.common.collect.Maps
import com.hazelcast.core.HazelcastInstance
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.codex.CodexApi
import com.openlattice.codex.CodexApi.Companion.BASE
import com.openlattice.codex.CodexApi.Companion.INCOMING
import com.openlattice.codex.CodexApi.Companion.ORG_ID
import com.openlattice.codex.CodexApi.Companion.ORG_ID_PATH
import com.openlattice.codex.CodexApi.Companion.STATUS
import com.openlattice.codex.CodexService
import com.openlattice.codex.MessageRequest
import com.openlattice.controllers.exceptions.BadRequestException
import com.openlattice.data.DataApi
import com.openlattice.datastore.apps.services.AppService
import com.openlattice.hazelcast.HazelcastQueue
import com.openlattice.notifications.sms.PhoneNumberService
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.postgres.mapstores.AppConfigMapstore
import com.openlattice.twilio.TwilioConfiguration
import com.twilio.Twilio
import com.twilio.rest.api.v2010.account.Message
import com.twilio.type.PhoneNumber
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.commons.lang.NotImplementedException
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.net.URI
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.Executors
import java.util.stream.Stream
import javax.inject.Inject
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

@SuppressFBWarnings(
        value = ["BC_BAD_CAST_TO_ABSTRACT_COLLECTION"],
        justification = "Allowing kotlin collection mapping cast to List")
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
        private val configuration: TwilioConfiguration,
        private val codexService: CodexService,
        private val phoneNumberService: PhoneNumberService
) : CodexApi, AuthorizingComponent {

    private val textingExecutor = Executors.newSingleThreadExecutor()
    private val twilioQueue = HazelcastQueue.TWILIO.getQueue(hazelcastInstance)
    val pendingTexts = Maps.newConcurrentMap<String, Message>()

    init {
        Twilio.init(configuration.sid, configuration.token)

        textingExecutor.execute {
            Stream.generate { twilioQueue.take() }.forEach { (organizationId, messageEntitySetId, messageContents, toPhoneNumber) ->
                //Not very efficient.
                val phone = organizations.getOrganization(organizationId)!!.smsEntitySetInfo
                        .flatMap { (phoneNumber, _, entitySetIds, _) -> entitySetIds.map { it to phoneNumber } }
                        .toMap()
                        .getValue(messageEntitySetId)

                if (phone == "") {
                    throw BadRequestException("No source phone number set for organization!")
                }
                val message = Message.creator(PhoneNumber(toPhoneNumber), PhoneNumber(phone), messageContents)
                        .setStatusCallback(URI.create("https://api.openlattice.com$BASE$INCOMING/$organizationId$STATUS")).create()
                pendingTexts[message.sid] = message
                codexService.processOutgoingMessage(message, organizationId)
            }
        }
    }

    @Timed
    @RequestMapping(path = ["", "/"], method = [RequestMethod.POST])
    override fun sendOutgoingText(@RequestBody contents: MessageRequest) {
        twilioQueue.put(contents)
    }

    @Timed
    @RequestMapping(path = [INCOMING + ORG_ID_PATH], method = [RequestMethod.POST], consumes = [MediaType.APPLICATION_FORM_URLENCODED_VALUE])
    fun receiveIncomingText(@PathVariable(ORG_ID) organizationId: UUID, request: HttpServletRequest) {

        val messageId = codexService.getIncomingMessageField(request, CodexConstants.Request.SID)
        val phoneNumber = codexService.getIncomingMessageField(request, CodexConstants.Request.FROM)
        val text = codexService.getIncomingMessageField(request, CodexConstants.Request.BODY)
        val dateTime = OffsetDateTime.now()

        codexService.processMessage(organizationId, dateTime, phoneNumber, messageId, text, isOutgoing = false)
    }

    @Timed
    @RequestMapping(path = [INCOMING + ORG_ID_PATH + STATUS], method = [RequestMethod.POST])
    fun listenForTextStatus(@PathVariable(ORG_ID) organizationId: UUID, request: HttpServletRequest) {

        val messageId = codexService.getIncomingMessageField(request, CodexConstants.Request.SID)
        val status = Message.Status.forValue(codexService.getIncomingMessageField(request, CodexConstants.Request.STATUS))

        codexService.updateMessageStatus(organizationId, messageId, status)
        if (status == Message.Status.FAILED || status == Message.Status.UNDELIVERED) {
            println("Message not received or even failed to send!!! ")
        } else {
            pendingTexts.remove(messageId)
        }
    }

    override fun receiveIncomingText(organizationId: UUID) {
        throw NotImplementedException("This should not be called without a HttpServletRequest")
    }

    override fun listenForTextStatus() {
        throw NotImplementedException("This should not be called without a HttpServletRequest")
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }

}