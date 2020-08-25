package com.openlattice.search.renderers

import com.google.common.collect.Maps
import com.openlattice.data.requests.NeighborEntityDetails
import com.openlattice.mail.RenderableEmailRequest
import com.openlattice.search.requests.PersistentSearch
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.time.OffsetDateTime
import java.util.*

private const val FROM_EMAIL = "courier@openlattice.com"
private const val TEMPLATE_PATH = "mail/templates/shared/CAREIssueAlertTemplate.mustache"

/* FQNs */
private val PERSON_ID_FQN = FullQualifiedName("nc.SubjectIdentification")
private val FIRST_NAME_FQN = FullQualifiedName("nc.PersonGivenName")
private val MIDDLE_NAME_FQN = FullQualifiedName("nc.PersonMiddleName")
private val LAST_NAME_FQN = FullQualifiedName("nc.PersonSurName")
private val STATUS_FQN = FullQualifiedName("ol.status")
private val DESCRIPTION_FQN = FullQualifiedName("ol.description")
private val PRIORITY_FQN = FullQualifiedName("ol.priority")
private val CATEGORY_FQN = FullQualifiedName("ol.category")
private val TITLE_FQN = FullQualifiedName("ol.title")
private val COMPLETED_DATE_TIME_FQN = FullQualifiedName("date.completeddatetime")
private val ENTRY_UPDATED_FQN = FullQualifiedName("general.entryupdated")
private val OPENLATTICE_ID_FQN = FullQualifiedName("openlattice.@id")


/* METADATA TAGS */
private const val PERSON_ENTITY_SET_ID_METADATA = "personEntitySetId"
private const val STAFF_ENTITY_SET_ID_METADATA = "staffEntitySetId"
private const val TIME_ZONE_METADATA = "timezone"
private const val ASSIGNED_TO_ENTITY_SET_ID_METADATA = "assignedToEntitySetId"
private const val REPORTED_ENTITY_SET_ID_METADATA = "reportedEntitySetId"

/*
*
* Search metadata is expected to contain the following fields:
* {
*   "personEntitySetId": <UUID>,
*   "staffEntitySetId": <UUID>,
*   "assignedToEntitySetID": <UUID>,
*   "reportedEntitySetID": <UUID>,
*   "timezone": <TimeZones>,
*   "alertName": <String>,
* }
*
*/

class CAREIssueAlertEmailRenderer {

    companion object {

        private fun isMatchByEntitySetId(neighbor :NeighborEntityDetails, entitySetId: UUID, associationEntitySetId: Optional<UUID>): Boolean {
            val isMatchingEntitySetId = neighbor.neighborEntitySet.isPresent && neighbor.neighborEntitySet.get().id == entitySetId
            return when (associationEntitySetId.isPresent) {
                (neighbor.associationEntitySet.id == associationEntitySetId.get()) && isMatchingEntitySetId -> isMatchingEntitySetId
                else -> isMatchingEntitySetId
            }
        }

        private fun getCombinedNeighbors(
                neighbors: List<NeighborEntityDetails>, entitySetId: UUID, associationEntitySetId: Optional<UUID>
        ): Map<FullQualifiedName, Set<Any>> {
            val entity = mutableMapOf<FullQualifiedName, MutableSet<Any>>()
            neighbors.filter { isMatchByEntitySetId(it, entitySetId, associationEntitySetId) }.forEach {
                it.neighborDetails.get().forEach { (fqn, values) ->
                    entity.getOrPut(fqn) { mutableSetOf() }.addAll(values)
                }
            }

            return entity
        }

        private fun getPersonDetails(
                neighbors: List<NeighborEntityDetails>, personEntitySetId: UUID
        ): Map<String, Any> {
            val combinedEntity = getCombinedNeighbors(neighbors, personEntitySetId, Optional.empty())

            val tags = mutableMapOf<String, String>()

            val firstName = (combinedEntity[FIRST_NAME_FQN] ?: emptySet()).joinToString("/")
            val lastName = (combinedEntity[LAST_NAME_FQN] ?: emptySet()).joinToString("/")
            val middleName = (combinedEntity[MIDDLE_NAME_FQN] ?: emptySet()).joinToString("/")
            tags["formattedName"] = "$lastName, $firstName $middleName"

            return tags
        }

        private fun getReporterDetails(
                neighbors: List<NeighborEntityDetails>, staffEntitySetId: UUID, reporterEntitySetId: UUID
        ): Map<String, Any> {
            val combinedEntity = getCombinedNeighbors(neighbors, staffEntitySetId, Optional.of(reporterEntitySetId))

            val tags = mutableMapOf<String, String>()

            tags["reporter"] = (combinedEntity[PERSON_ID_FQN] ?: emptySet()).joinToString(", ")

            return tags
        }

        private fun getAssigneeDetails(
                neighbors: List<NeighborEntityDetails>, staffEntitySetId: UUID, assigneeEntitySetId: UUID
        ): Map<String, Any> {
            val combinedEntity = getCombinedNeighbors(neighbors, staffEntitySetId, Optional.of(assigneeEntitySetId))

            val tags = mutableMapOf<String, String>()

            tags["assignee"] = (combinedEntity[PERSON_ID_FQN] ?: emptySet()).joinToString(", ")

            return tags
        }

        private fun getIssueDetails(
                issue: Map<FullQualifiedName, Set<Any>>, timeZone: MessageFormatters.TimeZones
        ): Map<String, Any> {
            val tags = mutableMapOf<String, String>()

            val completedDateTime = (issue[COMPLETED_DATE_TIME_FQN] ?: emptySet()).map { OffsetDateTime.parse(it.toString()) }
            val createdDate = completedDateTime.joinToString(", ") { MessageFormatters.formatDate(it, timeZone) }
            val createdTime = completedDateTime.joinToString(", ") { MessageFormatters.formatTime(it, timeZone) }
            tags["createdDateTime"] = "$createdDate $createdTime"

            val updatedDateTime = (issue[ENTRY_UPDATED_FQN] ?: emptySet()).map { OffsetDateTime.parse(it.toString()) }
            val updatedDate = updatedDateTime.joinToString(", ") { MessageFormatters.formatDate(it, timeZone) }
            val updatedTime = updatedDateTime.joinToString(", ") { MessageFormatters.formatTime(it, timeZone) }
            tags["updatedDateTime"] = "$updatedDate $updatedTime"

            tags["status"] = (issue[STATUS_FQN] ?: emptySet()).joinToString(", ")
            tags["title"] = (issue[TITLE_FQN] ?: emptySet()).joinToString(", ")
            tags["description"] = (issue[DESCRIPTION_FQN] ?: emptySet()).joinToString(", ")
            tags["category"] = (issue[CATEGORY_FQN] ?: emptySet()).joinToString(", ")
            tags["priority"] = (issue[PRIORITY_FQN] ?: emptySet()).joinToString(", ")
            tags["issueId"] = (issue[OPENLATTICE_ID_FQN] ?: emptySet()).joinToString(", ")

            return tags
        }

        fun renderEmail(
                persistentSearch: PersistentSearch,
                issue: Map<FullQualifiedName, Set<Any>>,
                userEmail: String,
                neighbors: List<NeighborEntityDetails>
        ): RenderableEmailRequest {

            val templateObjects: MutableMap<String, Any> = Maps.newHashMap<String, Any>()

            val personEntitySetId = UUID.fromString(
                    persistentSearch.alertMetadata[PERSON_ENTITY_SET_ID_METADATA].toString()
            )
            val staffEntitySetId = UUID.fromString(
                    persistentSearch.alertMetadata[STAFF_ENTITY_SET_ID_METADATA].toString()
            )
            val assignedToEntitySetId = UUID.fromString(
                    persistentSearch.alertMetadata[ASSIGNED_TO_ENTITY_SET_ID_METADATA].toString()
            )
            val reportedEntitySetId = UUID.fromString(
                    persistentSearch.alertMetadata[REPORTED_ENTITY_SET_ID_METADATA].toString()
            )
            val timezone = MessageFormatters.TimeZones.valueOf(persistentSearch.alertMetadata[TIME_ZONE_METADATA].toString())
            val issueDetails = getIssueDetails(issue, timezone)
            val subtitle = issueDetails["title"]?.let { ": $it" } ?: ""
            templateObjects["subtitle"] = subtitle
            val subject = "CARE Issue$subtitle"

            templateObjects.putAll(getPersonDetails(neighbors, personEntitySetId))
            templateObjects.putAll(getAssigneeDetails(neighbors, staffEntitySetId, assignedToEntitySetId))
            templateObjects.putAll(getReporterDetails(neighbors, staffEntitySetId, reportedEntitySetId))
            templateObjects.putAll(issueDetails)
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
