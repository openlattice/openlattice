package com.openlattice.search.renderers

import com.google.common.collect.Lists
import com.google.common.collect.Maps
import com.openlattice.data.requests.NeighborEntityDetails
import com.openlattice.mail.RenderableEmailRequest
import com.openlattice.search.requests.PersistentSearch
import jodd.mail.EmailAttachment
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.net.URL
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import javax.imageio.ImageIO

private const val FROM_EMAIL = "courier@openlattice.com"
private const val TEMPLATE_PATH = "mail/templates/shared/CodexMessageAlertTemplate.mustache"

private val PHONE_NUMBER_FQN = FullQualifiedName("contact.phonenumber")
private val MESSAGE_TEXT_FQN = FullQualifiedName("ol.text")
private val DATETIME_FQN = FullQualifiedName("general.datetime")
private val ATTACHMENT_FQN = FullQualifiedName("ol.imagedata")


/*
*
* Search metadata is expected to contain the following fields:
* {
*   "personEntitySetId": <UUID>,
*   "staffEntitySetId": <UUID>,
*   "timezone": <BHRTimeZone>
* }
*
*/

private const val TIME_ZONE_METADATA = "timezone"
private const val CONVERSATION_URL_METADATA = "conversationURL"

private const val TEXT_FIELD = "text"
private const val PHONE_NUMBER_FIELD = "phoneNumber"
private const val DATE_TIME_FIELD = "dateTime"

class CodexAlertEmailRenderer {

    companion object {

        private fun getStringValue(entity: Map<FullQualifiedName, Set<Any>>, fqn: FullQualifiedName): String {
            return (entity[fqn] ?: emptySet()).joinToString(", ")
        }

        private fun getMessageDetails(message: Map<FullQualifiedName, Set<Any>>, timezone: MessageFormatters.TimeZones): Map<String, Any> {
            val tags = mutableMapOf<String, Any>()

            tags[TEXT_FIELD] = getStringValue(message, MESSAGE_TEXT_FQN)
            tags[PHONE_NUMBER_FIELD] = getStringValue(message, PHONE_NUMBER_FQN)

            (message[DATETIME_FQN] ?: emptySet()).map {
                val dateTime = OffsetDateTime.parse(it.toString())
                tags[DATE_TIME_FIELD] = "${MessageFormatters.formatDate(dateTime, timezone)} at ${MessageFormatters.formatTime(dateTime, timezone)}"
            }

            return tags
        }

        fun getMetadataTemplateObjects(alertMetadata: Map<String, Any>): Map<String, Any> {
            val tags = mutableMapOf<String, Any>()

            tags[CONVERSATION_URL_METADATA] = alertMetadata[CONVERSATION_URL_METADATA]?.let { "<a href=\"$it\">$it</a>" } ?: ""

            return tags
        }

        fun renderEmail(
                persistentSearch: PersistentSearch,
                message: Map<FullQualifiedName, Set<Any>>,
                userEmail: String
        ): RenderableEmailRequest {

            val templateObjects: MutableMap<String, Any> = mutableMapOf()

            val timezone = MessageFormatters.TimeZones.valueOf(persistentSearch.alertMetadata[TIME_ZONE_METADATA].toString())

            templateObjects.putAll(getMessageDetails(message, timezone))
            templateObjects.putAll(getMetadataTemplateObjects(persistentSearch.alertMetadata))

            val subject = "New Text Message From ${templateObjects[PHONE_NUMBER_FIELD]}"

            templateObjects["subscriber"] = userEmail

            return RenderableEmailRequest(
                    Optional.of(FROM_EMAIL),
                    arrayOf(userEmail) + persistentSearch.additionalEmailAddresses,
                    Optional.empty(),
                    Optional.empty(),
                    TEMPLATE_PATH,
                    Optional.of(subject),
                    Optional.of(templateObjects),
                    Optional.empty(),
                    Optional.empty()
            )
        }
    }

}