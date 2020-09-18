package com.openlattice.codex.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.authorization.*
import com.openlattice.codex.CodexApi
import com.openlattice.codex.CodexApi.Companion.ID
import com.openlattice.codex.CodexApi.Companion.ID_PATH
import com.openlattice.codex.CodexApi.Companion.INCOMING
import com.openlattice.codex.CodexApi.Companion.MEDIA
import com.openlattice.codex.CodexApi.Companion.ORG_ID
import com.openlattice.codex.CodexApi.Companion.ORG_ID_PATH
import com.openlattice.codex.CodexApi.Companion.PHONE
import com.openlattice.codex.CodexApi.Companion.PHONE_PATH
import com.openlattice.codex.CodexApi.Companion.SCHEDULED
import com.openlattice.codex.CodexApi.Companion.STATUS
import com.openlattice.codex.CodexService
import com.openlattice.codex.MessageRequest
import com.openlattice.collections.CollectionsManager
import com.openlattice.controllers.exceptions.BadRequestException
import com.openlattice.data.DataApi
import com.openlattice.data.DataEdgeKey
import com.openlattice.apps.services.AppService
import com.openlattice.controllers.exceptions.ForbiddenException
import com.openlattice.twilio.TwilioConfiguration
import com.twilio.rest.api.v2010.account.Message
import com.twilio.security.RequestValidator
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.commons.lang3.NotImplementedException
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.*
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
        private val twilioConfiguration: TwilioConfiguration,
        private val authorizationManager: AuthorizationManager,
        private val codexService: CodexService
) : CodexApi, AuthorizingComponent {

    companion object {
        private val logger = LoggerFactory.getLogger(CodexController::class.java)!!
    }

    private val validator = RequestValidator(twilioConfiguration.token)

    @Timed
    @RequestMapping(path = ["", "/"], method = [RequestMethod.POST])
    override fun sendOutgoingText(@RequestBody contents: MessageRequest) {
        ensureWriteAccess(AclKey(contents.messageEntitySetId))
        contents.senderId = Principals.getCurrentUser().id
        codexService.scheduleOutgoingMessage(contents)
    }

    @Timed
    @RequestMapping(path = [INCOMING + ORG_ID_PATH], method = [RequestMethod.POST], consumes = [MediaType.APPLICATION_FORM_URLENCODED_VALUE])
    @ResponseStatus(HttpStatus.NO_CONTENT)
    fun receiveIncomingText(@PathVariable(ORG_ID) organizationId: UUID, request: HttpServletRequest) {
        ensureTwilio(request)
        codexService.processIncomingMessage(organizationId, request)
    }

    @Timed
    @RequestMapping(path = [INCOMING + ORG_ID_PATH + STATUS], method = [RequestMethod.POST])
    @ResponseStatus(HttpStatus.NO_CONTENT)
    fun listenForTextStatus(@PathVariable(ORG_ID) organizationId: UUID, request: HttpServletRequest) {

        ensureTwilio(request)

        val messageId = request.getParameter(CodexConstants.Request.SID.parameter)
        val status = Message.Status.forValue(request.getParameter(CodexConstants.Request.STATUS.parameter))

        codexService.updateMessageStatus(organizationId, messageId, status)

        if (status == Message.Status.FAILED || status == Message.Status.UNDELIVERED) {
            logger.error("Message $messageId not received or even failed to send!!! ")
        }
    }

    @Timed
    @RequestMapping(path = [MEDIA + ID_PATH], method = [RequestMethod.GET])
    fun readAndDeleteMedia(@PathVariable(ID) mediaId: UUID, response: HttpServletResponse): ByteArray {
        val base64Media = codexService.getAndDeleteMedia(mediaId)
        response.contentType = base64Media.contentType
        return Base64.getDecoder().decode(base64Media.data)
    }

    @Timed
    @RequestMapping(path = [SCHEDULED + ORG_ID_PATH], method = [RequestMethod.GET])
    override fun getUpcomingScheduledMessages(@PathVariable(ORG_ID) organizationId: UUID): Map<UUID, MessageRequest> {
        return getMessagesFilteredByPermissions(
                codexService.getScheduledMessagesForOrganization(organizationId)
        )
    }

    @Timed
    @RequestMapping(path = [SCHEDULED + ORG_ID_PATH + PHONE_PATH], method = [RequestMethod.GET])
    override fun getUpcomingScheduledMessagesToPhoneNumber(
            @PathVariable(ORG_ID) organizationId: UUID,
            @PathVariable(PHONE) phone: String
    ): Map<UUID, MessageRequest> {
        return getMessagesFilteredByPermissions(
                codexService.getScheduledMessagesForOrganizationAndPhoneNumber(organizationId, phone)
        )
    }

    @Timed
    @RequestMapping(path = [SCHEDULED + ID_PATH], method = [RequestMethod.DELETE])
    override fun cancelScheduledMessage(@PathVariable(ID) messageId: UUID) {
        val message = codexService.getMessageRequest(messageId)
        ensureWriteAccess(AclKey(message.messageEntitySetId))
        codexService.deleteScheduledTask(messageId)
    }

    fun ensureTwilio(request: HttpServletRequest) {

        val url = "${twilioConfiguration.callbackBaseUrl}${request.requestURI}"
        val signature = request.getHeader("X-Twilio-Signature")
        val params = request.parameterMap.mapValues { request.getParameter(it.key) }

        if (!validator.validate(url, params, signature)) {
            throw ForbiddenException("Could not verify that incoming request to $url was sent by Twilio")
        }

    }

    private fun getMessagesFilteredByPermissions(messages: Map<UUID, MessageRequest>): Map<UUID, MessageRequest> {
        val entitySetIds = messages.values.map { it.messageEntitySetId }.toSet()
        val requests = entitySetIds.associate { AclKey(it) to EnumSet.of(Permission.READ) }

        val authorizedEntitySetIds = authorizationManager.authorize(requests, Principals.getCurrentPrincipals()).filterValues { permissionMap ->
            permissionMap.getValue(Permission.READ)
        }.map { aclKeyEntry -> aclKeyEntry.key[0] }.toSet()

        return messages.filterValues { authorizedEntitySetIds.contains(it.messageEntitySetId) }
    }

    override fun receiveIncomingText(organizationId: UUID) {
        throw NotImplementedException("This should not be called without a HttpServletRequest")
    }

    override fun listenForTextStatus() {
        throw NotImplementedException("This should not be called without a HttpServletRequest")
    }

    override fun readAndDeleteMedia(mediaId: UUID) {
        throw NotImplementedException("This should not be called without a HttpServletRequest")
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }

}