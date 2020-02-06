package com.openlattice.codex.controllers

import com.codahale.metrics.annotation.Timed
import com.hazelcast.core.HazelcastInstance
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.authorization.Principals
import com.openlattice.codex.CodexApi
import com.openlattice.codex.CodexApi.Companion.INCOMING
import com.openlattice.codex.CodexApi.Companion.ORG_ID
import com.openlattice.codex.CodexApi.Companion.ORG_ID_PATH
import com.openlattice.codex.CodexApi.Companion.STATUS
import com.openlattice.codex.CodexService
import com.openlattice.codex.MessageRequest
import com.openlattice.hazelcast.HazelcastQueue
import com.twilio.rest.api.v2010.account.Message
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.commons.lang.NotImplementedException
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.inject.Inject
import javax.servlet.http.HttpServletRequest

@SuppressFBWarnings(
        value = ["BC_BAD_CAST_TO_ABSTRACT_COLLECTION"],
        justification = "Allowing kotlin collection mapping cast to List")
@RestController
@RequestMapping(CodexApi.CONTROLLER)
class CodexController
@Inject
constructor(
        hazelcastInstance: HazelcastInstance,
        private val authorizationManager: AuthorizationManager,
        private val codexService: CodexService
) : CodexApi, AuthorizingComponent {

    private val twilioQueue = HazelcastQueue.TWILIO.getQueue(hazelcastInstance)

    @Timed
    @RequestMapping(path = ["", "/"], method = [RequestMethod.POST])
    override fun sendOutgoingText(@RequestBody contents: MessageRequest) {
        ensureWriteAccess(AclKey(contents.messageEntitySetId))
        contents.senderId = Principals.getCurrentUser().id
        twilioQueue.put(contents)
    }

    @Timed
    @RequestMapping(path = [INCOMING + ORG_ID_PATH], method = [RequestMethod.POST], consumes = [MediaType.APPLICATION_FORM_URLENCODED_VALUE])
    fun receiveIncomingText(@PathVariable(ORG_ID) organizationId: UUID, request: HttpServletRequest) {
        codexService.processIncomingMessage(organizationId, request)
    }

    @Timed
    @RequestMapping(path = [INCOMING + ORG_ID_PATH + STATUS], method = [RequestMethod.POST])
    fun listenForTextStatus(@PathVariable(ORG_ID) organizationId: UUID, request: HttpServletRequest) {

        val messageId = request.getParameter(CodexConstants.Request.SID.parameter)
        val status = Message.Status.forValue(request.getParameter(CodexConstants.Request.STATUS.parameter))

        codexService.updateMessageStatus(organizationId, messageId, status)

        if (status == Message.Status.FAILED || status == Message.Status.UNDELIVERED) {
            println("Message $messageId not received or even failed to send!!! ")
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