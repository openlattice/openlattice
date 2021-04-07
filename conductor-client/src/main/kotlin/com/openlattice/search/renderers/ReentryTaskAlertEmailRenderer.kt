package com.openlattice.search.renderers

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
private val CATEGORY_FQN = FullQualifiedName("ol.category")
private val TITLE_FQN = FullQualifiedName("ol.title")
private val GENERAL_DATETIME_FQN = FullQualifiedName("general.datetime")


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
*   "assignedToEntitySetId": <UUID>,
*   "reportedEntitySetId": <UUID>,
*   "timezone": <TimeZones>,
* }
*
*/

class ReentryTaskAlertEmailRenderer {

    companion object {

        private fun isMatchByEntitySetId(neighbor: NeighborEntityDetails, entitySetId: UUID, associationEntitySetId: Optional<UUID>): Boolean {

            if (neighbor.neighborEntitySet.isEmpty) {
              return false
            }

            if (neighbor.neighborEntitySet.get().id != entitySetId) {
              return false
            }

            if (associationEntitySetId.isPresent && associationEntitySetId.get() != neighbor.associationEntitySet.id) {
              return false
            }

            return true
        }

        private fun getCombinedNeighbors(
                neighbors: List<NeighborEntityDetails>, entitySetId: UUID, associationEntitySetId: Optional<UUID>
        ): Map<FullQualifiedName, Set<Any>> {
            val entity = mutableMapOf<FullQualifiedName, MutableSet<Any>>()
            neighbors.filter { isMatchByEntitySetId(it, entitySetId, associationEntitySetId) }.forEach {
                it.neighborDetails.get().forEach { (fqn, values) ->
                    entity.getOrPut(fqn) { HashSet(values.size) }.addAll(values)
                }
            }

            return entity
        }

        private fun getPersonDetails(
                neighbors: List<NeighborEntityDetails>, personEntitySetId: UUID
        ): Map<String, String> {
            val combinedEntity = getCombinedNeighbors(neighbors, personEntitySetId, Optional.empty())

            val firstName = (combinedEntity[FIRST_NAME_FQN] ?: emptySet()).joinToString("/")
            val lastName = (combinedEntity[LAST_NAME_FQN] ?: emptySet()).joinToString("/")
            val middleName = (combinedEntity[MIDDLE_NAME_FQN] ?: emptySet()).joinToString("/")
            val formattedName = "$lastName, $firstName $middleName"

            return mapOf("formattedName" to formattedName)
        }

        private fun getReporterDetails(
                neighbors: List<NeighborEntityDetails>, staffEntitySetId: UUID, reporterEntitySetId: UUID
        ): Map<String, String> {
            val combinedEntity = getCombinedNeighbors(neighbors, staffEntitySetId, Optional.of(reporterEntitySetId))

            return mapOf("reporter" to (combinedEntity[PERSON_ID_FQN] ?: emptySet()).joinToString(", "))
        }

        private fun getAssigneeDetails(
                neighbors: List<NeighborEntityDetails>, staffEntitySetId: UUID, assigneeEntitySetId: UUID
        ): Map<String, String> {
            val combinedEntity = getCombinedNeighbors(neighbors, staffEntitySetId, Optional.of(assigneeEntitySetId))

            return mapOf("assignee" to (combinedEntity[PERSON_ID_FQN] ?: emptySet()).joinToString(", "))
        }

        private fun getIssueDetails(
                issue: Map<FullQualifiedName, Set<Any>>, timeZone: MessageFormatters.TimeZones
        ): Map<String, String> {

            val dueDateTime = (issue[GENERAL_DATETIME_FQN] ?: emptySet()).map { OffsetDateTime.parse(it.toString()) }
            val dueDate = dueDateTime.joinToString(", ") { MessageFormatters.formatDate(it, timeZone) }
            val dueTime = dueDateTime.joinToString(", ") { MessageFormatters.formatTime(it, timeZone) }
            val dueDateString = "$dueDate $dueTime"
            val status = (issue[STATUS_FQN] ?: emptySet()).joinToString(", ")
            val title = (issue[TITLE_FQN] ?: emptySet()).joinToString(", ")
            val description = (issue[DESCRIPTION_FQN] ?: emptySet()).joinToString(", ")
            val category = (issue[CATEGORY_FQN] ?: emptySet()).joinToString(", ")

            return mapOf(
                    "dueDateTime" to dueDateString,
                    "status" to status,
                    "title" to title,
                    "description" to description,
                    "category" to category
            )
        }

        fun renderEmail(
                persistentSearch: PersistentSearch,
                issue: Map<FullQualifiedName, Set<Any>>,
                userEmail: String,
                neighbors: List<NeighborEntityDetails>
        ): RenderableEmailRequest {

            val templateObjects = mutableMapOf<String, Any>()

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
            val subject = "Reentry Task$subtitle"

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
