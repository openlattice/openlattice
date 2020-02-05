package com.openlattice.codex

import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.ListMultimap
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.openlattice.apps.AppConfigKey
import com.openlattice.apps.AppTypeSetting
import com.openlattice.codex.controllers.CodexConstants
import com.openlattice.data.DataEdge
import com.openlattice.data.DataGraphManager
import com.openlattice.data.EntityDataKey
import com.openlattice.data.EntityKeyIdService
import com.openlattice.datastore.apps.services.AppService
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.notifications.sms.PhoneNumberService
import com.twilio.rest.api.v2010.account.Message
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset.UTC
import java.util.*
import javax.servlet.http.HttpServletRequest

class CodexService(
        val hazelcast: HazelcastInstance,
        val appService: AppService,
        val edmManager: EdmManager,
        val dataGraphManager: DataGraphManager,
        val entityKeyIdService: EntityKeyIdService,
        val phoneNumberService: PhoneNumberService
) {

    companion object {
        private val logger = LoggerFactory.getLogger(CodexService::class.java)
    }

    val appId = appService.getApp(CodexConstants.APP_NAME).id
    val typesByFqn = CodexConstants.Types.values().associate { it to appService.getAppType(it.fqn) }
    val appConfigs: IMap<AppConfigKey, AppTypeSetting> = HazelcastMap.APP_CONFIGS.getMap(hazelcast)
    val propertyTypesByAppType = typesByFqn.values.associate { it.id to edmManager.getPropertyTypesOfEntityType(it.entityTypeId) }
    val propertyTypesByFqn = propertyTypesByAppType.values.flatMap { it.values }.associate { it.type to it.id } // TODO check if collisions break this

    fun processOutgoingMessage(message: Message, organizationId: UUID) {
        val dateTime = formatDateTime(message.dateCreated)
        val phoneNumber = message.to
        val messageId = message.sid
        val text = message.body

        processMessage(organizationId, dateTime, phoneNumber, messageId, text, isOutgoing = true)
    }

    fun getIncomingMessageField(request: HttpServletRequest, field: CodexConstants.Request): String {
        return request.getParameter(field.parameter)
    }

    fun processMessage(
            organizationId: UUID,
            dateTime: OffsetDateTime,
            phoneNumber: String,
            messageId: String,
            text: String, isOutgoing: Boolean
    ) {
        /* create entities */

        val contactEDK = getContactEntityDataKey(organizationId, phoneNumber)
        val messageEDK = getMessageEntityDataKey(organizationId, dateTime, messageId, text)

        /* create associations */

        val associations: ListMultimap<UUID, DataEdge> = ArrayListMultimap.create()

        val edgeType = if (isOutgoing) CodexConstants.Types.SENT_TO else CodexConstants.Types.SENT_FROM
        val edgeEntitySetId = getEntitySetId(organizationId, edgeType)

        associations.put(edgeEntitySetId, DataEdge(messageEDK, contactEDK, mapOf(getPropertyTypeId(CodexConstants.Properties.DATE_TIME) to setOf(dateTime))))

        dataGraphManager.createAssociations(associations, mapOf(edgeEntitySetId to getPropertyTypes(edgeType)))
    }

    fun updateMessageStatus(organizationId: UUID, messageId: String, status: Message.Status) {

        val wasDelivered = when (status) {
            Message.Status.DELIVERED -> true
            Message.Status.UNDELIVERED -> false
            Message.Status.FAILED -> false
            else -> return
        }

        val messageEntitySetId = getEntitySetId(organizationId, CodexConstants.Types.MESSAGES)
        val messageEntityKeyId = entityKeyIdService.getEntityKeyId(messageEntitySetId, messageId)

        dataGraphManager.partialReplaceEntities(
                messageEntitySetId,
                mapOf(messageEntityKeyId to mapOf(getPropertyTypeId(CodexConstants.Properties.WAS_DELIVERED) to setOf(wasDelivered))),
                getPropertyTypes(CodexConstants.Types.MESSAGES)
        )
    }


    fun getIncomingMessageOrganizationId(message: Message): UUID {
        return phoneNumberService.lookup(message.to).first().organizationId
    }

    private fun getContactEntityDataKey(organizationId: UUID, phoneNumber: String): EntityDataKey {
        val contactEntitySetId = getEntitySetId(organizationId, CodexConstants.Types.CONTACT_INFO)
        val contactEntityKeyId = entityKeyIdService.getEntityKeyId(contactEntitySetId, phoneNumber)

        return EntityDataKey(contactEntitySetId, contactEntityKeyId)
    }

    private fun getMessageEntityDataKey(organizationId: UUID, dateTime: OffsetDateTime, messageId: String, text: String): EntityDataKey {

        val messageEntity = mapOf(
                getPropertyTypeId(CodexConstants.Properties.ID) to setOf(messageId),
                getPropertyTypeId(CodexConstants.Properties.DATE_TIME) to setOf(dateTime),
                getPropertyTypeId(CodexConstants.Properties.TEXT) to setOf(text)
        )
        val messageEntitySetId = getEntitySetId(organizationId, CodexConstants.Types.MESSAGES)
        val messageEntityKeyId = entityKeyIdService.getEntityKeyId(messageEntitySetId, messageId)

        dataGraphManager.mergeEntities(
                messageEntitySetId,
                mapOf(messageEntityKeyId to messageEntity),
                getPropertyTypes(CodexConstants.Types.MESSAGES)
        )

        return EntityDataKey(messageEntitySetId, messageEntityKeyId)
    }

    private fun getEntitySetId(organizationId: UUID, type: CodexConstants.Types): UUID {
        val appTypeId = typesByFqn.getValue(type).id
        val ack = AppConfigKey(appId, organizationId, appTypeId)
        return appConfigs[ack]!!.entitySetId
    }

    private fun getPropertyTypes(type: CodexConstants.Types): Map<UUID, PropertyType> {
        val appTypeId = typesByFqn.getValue(type).id
        return propertyTypesByAppType.getValue(appTypeId)
    }

    private fun formatDateTime(dateTime: DateTime): OffsetDateTime {
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(dateTime.toInstant().millis), UTC)
    }

    private fun getPropertyTypeId(property: CodexConstants.Properties): UUID {
        return propertyTypesByFqn.getValue(property.fqn)
    }


}