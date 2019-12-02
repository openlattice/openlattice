package com.openlattice.search.renderers

import com.google.common.collect.Maps
import com.openlattice.data.requests.NeighborEntityDetails
import com.openlattice.mail.RenderableEmailRequest
import com.openlattice.search.requests.PersistentSearch
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.*

private const val FROM_EMAIL = "courier@openlattice.com"
private const val TEMPLATE_PATH = "mail/templates/shared/BHRAlertTemplate.mustache"

/* TIME ZONE SETUP */
private enum class BHRTimeZone {
    PST,
    MST,
    CST,
    EST
}

private val DATE_FORMAT = "MM/dd/yyyy"
private val TIME_FORMAT = "hh:mm a, z"

private val DATE_FORMATTERS = mapOf<BHRTimeZone, DateTimeFormatter>(
        Pair(
                BHRTimeZone.PST, DateTimeFormatter.ofPattern(DATE_FORMAT).withZone(
                TimeZone.getTimeZone("America/Los_Angeles").toZoneId()
        )
        ),
        Pair(
                BHRTimeZone.MST,
                DateTimeFormatter.ofPattern(DATE_FORMAT).withZone(TimeZone.getTimeZone("America/Denver").toZoneId())
        ),
        Pair(
                BHRTimeZone.CST,
                DateTimeFormatter.ofPattern(DATE_FORMAT).withZone(TimeZone.getTimeZone("America/Chicago").toZoneId())
        ),
        Pair(
                BHRTimeZone.EST,
                DateTimeFormatter.ofPattern(DATE_FORMAT).withZone(TimeZone.getTimeZone("America/New_York").toZoneId())
        )
)

private val TIME_FORMATTERS = mapOf<BHRTimeZone, DateTimeFormatter>(
        Pair(
                BHRTimeZone.PST, DateTimeFormatter.ofPattern(TIME_FORMAT).withZone(
                TimeZone.getTimeZone("America/Los_Angeles").toZoneId()
        )
        ),
        Pair(
                BHRTimeZone.MST,
                DateTimeFormatter.ofPattern(TIME_FORMAT).withZone(TimeZone.getTimeZone("America/Denver").toZoneId())
        ),
        Pair(
                BHRTimeZone.CST,
                DateTimeFormatter.ofPattern(TIME_FORMAT).withZone(TimeZone.getTimeZone("America/Chicago").toZoneId())
        ),
        Pair(
                BHRTimeZone.EST,
                DateTimeFormatter.ofPattern(TIME_FORMAT).withZone(TimeZone.getTimeZone("America/New_York").toZoneId())
        )
)

/* FQNs */
private val PERSON_ID_FQN = FullQualifiedName("nc.SubjectIdentification")
private val FIRST_NAME_FQN = FullQualifiedName("nc.PersonGivenName")
private val MIDDLE_NAME_FQN = FullQualifiedName("nc.PersonMiddleName")
private val LAST_NAME_FQN = FullQualifiedName("nc.PersonSurName")
private val ALIAS_FQN = FullQualifiedName("im.PersonNickName")
private val DOB_FQN = FullQualifiedName("nc.PersonBirthDate")
private val SEX_FQN = FullQualifiedName("nc.PersonSex")
private val RACE_FQN = FullQualifiedName("nc.PersonRace")
private val AGE_FQN = FullQualifiedName("bhr.age")
private val INCIDENT_DATE_TIME_FQN = FullQualifiedName("bhr.datetimeOccurred")
private val NATURE_OF_CRISIS_FQN = FullQualifiedName("bhr.dispatchReason")
private val HOUSING_SITUATION_FQN = FullQualifiedName("housing.living_arrangements")
private val DISPOSITION_FQN = FullQualifiedName("bhr.disposition")


/* METADATA TAGS */
private const val PERSON_ENTITY_SET_ID_METADATA = "personEntitySetId"
private const val STAFF_ENTITY_SET_ID_METADATA = "staffEntitySetId"
private const val TIME_ZONE_METADATA = "timezone"

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

class BHRAlertEmailRenderer {

    companion object {

        private fun getCombinedNeighbors(
                neighbors: List<NeighborEntityDetails>, entitySetId: UUID
        ): Map<FullQualifiedName, Set<Any>> {
            val entity = mutableMapOf<FullQualifiedName, MutableSet<Any>>()
            neighbors.filter { it.neighborEntitySet.isPresent && it.neighborEntitySet.get().id == entitySetId }.forEach {
                it.neighborDetails.get().forEach { (fqn, values) ->
                    entity.getOrPut(fqn) { mutableSetOf() }.addAll(values)
                }
            }

            return entity
        }

        private fun getPersonDetails(
                neighbors: List<NeighborEntityDetails>, personEntitySetId: UUID, timeZone: BHRTimeZone
        ): Map<String, Any> {
            val combinedEntity = getCombinedNeighbors(neighbors, personEntitySetId)

            val tags = mutableMapOf<String, String>()

            val firstName = (combinedEntity[FIRST_NAME_FQN] ?: emptySet()).joinToString("/")
            val lastName = (combinedEntity[LAST_NAME_FQN] ?: emptySet()).joinToString("/")
            val middleName = (combinedEntity[MIDDLE_NAME_FQN] ?: emptySet()).joinToString("/")
            tags["formattedName"] = "$lastName, $firstName $middleName"

            tags["aliases"] = (combinedEntity[ALIAS_FQN] ?: emptySet()).joinToString(", ")
            tags["race"] = (combinedEntity[RACE_FQN] ?: emptySet()).joinToString(", ")
            tags["sex"] = (combinedEntity[SEX_FQN] ?: emptySet()).joinToString(", ")
            tags["dob"] = (combinedEntity[DOB_FQN] ?: emptySet()).joinToString(", ") {
                LocalDate.parse(it.toString()).format(
                        DATE_FORMATTERS[timeZone]
                )
            }

            return tags
        }

        private fun getFilerDetails(neighbors: List<NeighborEntityDetails>, staffEntitySetId: UUID): Map<String, Any> {
            val combinedEntity = getCombinedNeighbors(neighbors, staffEntitySetId)

            val tags = mutableMapOf<String, String>()

            tags["lawEnforcementContact"] = (combinedEntity[PERSON_ID_FQN] ?: emptySet()).joinToString(", ")

            return tags
        }

        private fun getReportDetails(
                report: Map<FullQualifiedName, Set<Any>>, timeZone: BHRTimeZone
        ): Map<String, Any> {
            val tags = mutableMapOf<String, String>()

            val dates = (report[INCIDENT_DATE_TIME_FQN] ?: emptySet()).map { OffsetDateTime.parse(it.toString()) }
            tags["date"] = dates.joinToString(", ") { it.format(DATE_FORMATTERS[timeZone]) }
            tags["time"] = dates.joinToString(", ") { it.format(TIME_FORMATTERS[timeZone]) }

            tags["natureOfCrisis"] = (report[NATURE_OF_CRISIS_FQN] ?: emptySet()).joinToString(", ")
            tags["housingSituation"] = (report[HOUSING_SITUATION_FQN] ?: emptySet()).joinToString(", ")
            tags["disposition"] = (report[DISPOSITION_FQN] ?: emptySet()).joinToString(", ")
            tags["age"] = (report[AGE_FQN] ?: emptySet()).joinToString(", ")

            return tags
        }

        fun renderEmail(
                persistentSearch: PersistentSearch,
                report: Map<FullQualifiedName, Set<Any>>,
                userEmail: String,
                neighbors: List<NeighborEntityDetails>
        ): RenderableEmailRequest {

            val subject = "New Behavioral Health Report"

            val templateObjects: MutableMap<String, Any> = Maps.newHashMap<String, Any>()

            val personEntitySetId = UUID.fromString(
                    persistentSearch.alertMetadata[PERSON_ENTITY_SET_ID_METADATA].toString()
            )
            val staffEntitySetId = UUID.fromString(
                    persistentSearch.alertMetadata[STAFF_ENTITY_SET_ID_METADATA].toString()
            )
            val timezone = BHRTimeZone.valueOf(persistentSearch.alertMetadata[TIME_ZONE_METADATA].toString())

            templateObjects.putAll(getPersonDetails(neighbors, personEntitySetId, timezone))
            templateObjects.putAll(getFilerDetails(neighbors, staffEntitySetId))
            templateObjects.putAll(getReportDetails(report, timezone))

            return RenderableEmailRequest(
                    Optional.of(FROM_EMAIL),
                    arrayOf(userEmail),
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