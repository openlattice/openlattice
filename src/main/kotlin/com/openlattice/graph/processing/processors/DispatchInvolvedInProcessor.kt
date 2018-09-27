package com.openlattice.graph.processing.processors

import com.openlattice.analysis.requests.ValueFilter
import com.openlattice.postgres.DataTables
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.springframework.stereotype.Component

private const val involed_in = "ol.involvedin"
private const val involed_in_key = "ol.id"
private const val involed_in_type = ""//TODO()

private const val person_police_minutes = "ol.personpoliceminutes"
private const val person_fire_minutes = "ol.personfireminutes"
private const val person_ems_minutes = "ol.personemsminutes"


private const val general_person = "general.person"
private const val general_person_key = "nc.SubjectIdentification"


private const val dispatch = "ol.dispatch"
private const val dispatch_key = "ol.id"
private const val dispatch_duration = "ol.durationinterval"

private const val start = "ol.datetimestart"
private const val end = "ol.datetimeend"
private const val duration = "ol.organizationtime"

private const val num_of_people = "ol.numberofpeople"
private const val num_of_police_officers = "ol.numberofpolice"
private const val num_of_police_officers_no_duration = "ol.numberofpolicenoduration"
private const val num_of_police_units = "ol.numberofpoliceunits"
private const val num_of_police_units_no_duration = "ol.numberofpoliceunitsnoduration"
private const val num_of_fire = "ol.numberoffireunits"
private const val num_of_fire_no_duration = "ol.numberoffireunitsnoduration"
private const val num_of_ems = "ol.numberofpolice"
private const val num_of_ems_no_duration = "ol.numberofpolicenoduration"

private const val police_minutes = "ol.policeminutes"
private const val police_unit_minutes = "ol.policeunitminutes"
private const val fire_minutes = "ol.fireminutes"
private const val ems_minutes = "ol.emsminutes"


private const val police_unit = "ol.unit"
private const val police_unit_key = "ol.id"
private const val police_unit_name = "ol.name" // to avoid duplicate column names with ol.id

private const val fire_unit = "ol.fireunit"
private const val fire_unit_key = "ol.id"
private const val fire_unit_name = "ol.name" // to avoid duplicate column names with ol.id

private const val ems_unit = "ol.emsunit"
private const val ems_unit_key = "ol.id"
private const val ems_unit_name = "ol.name" // to avoid duplicate column names with ol.id


private fun getAssociationDurationCalc(edgeDurationFqn:String, eventDurationFqn:String):String {
    return "SUM(CASE WHEN (ARRAY_LENGTH(${DataTables.quote(edgeDurationFqn)}) IS null) " +
            "THEN ${DataTables.quote(eventDurationFqn)}[1] " +
            "ELSE ${DataTables.quote(edgeDurationFqn)}[1] END)"
}

private fun countOf(columnName: String): String {
    return "COUNT(${DataTables.quote(columnName)})"
}

private fun emptyInvolvedInDurationCount(): String {
    return "COUNT(CASE WHEN (ARRAY_LENGTH($duration) IS null) THEN 1 END)" // array_length should return null if empty
}


/* ************** Base duration calculation : on association ************* */
@Component
class DispatchInvolvedInProcessor: GraphProcessor, AssociationProcessor()  {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(FullQualifiedName(involed_in) to setOf(FullQualifiedName(start), FullQualifiedName(end)),
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_key)))
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(involed_in), FullQualifiedName(duration))
    }

    override fun getSql(): String {
        val firstStart = "(SELECT unnest(${DataTables.quote(start)}) ORDER BY 1 LIMIT 1)"
        val lastEnd = "(SELECT unnest(${DataTables.quote(end)}) ORDER BY 1 DESC LIMIT 1)"
        return "SUM(EXTRACT(epoch FROM ($lastEnd - $firstStart))/60)"
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}



/* ************** Per person, per unit duration calculation : on association ************* */

class DispatchInvolvedInPersonPoliceMinutesProcessor: GraphProcessor, AssociationProcessor() {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(
                    FullQualifiedName(police_minutes),
                    FullQualifiedName(police_unit_minutes),
                    FullQualifiedName(num_of_people)),
                FullQualifiedName(involed_in) to setOf(FullQualifiedName(involed_in_type)),
                FullQualifiedName(general_person) to setOf(FullQualifiedName(general_person_key)))
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(involed_in), FullQualifiedName(person_police_minutes))
    }

    override fun getSql(): String {
        return "(${DataTables.quote(police_minutes)}[1] + ${DataTables.quote(police_unit_minutes)}[1]) " +
                "/ ${DataTables.quote(num_of_people)}[1]"
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        //TODO: need valuefilter
        //return mapOf(FullQualifiedName(involed_in) to mapOf(FullQualifiedName(involed_in_type) to setOf(??????))
        return TODO("filter on involvedin type")
    }
}

class DispatchInvolvedInPersonFireMinutesProcessor: GraphProcessor, AssociationProcessor() {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(
                        FullQualifiedName(fire_minutes),
                        FullQualifiedName(num_of_people)),
                FullQualifiedName(involed_in) to setOf(FullQualifiedName(involed_in_type)),
                FullQualifiedName(general_person) to setOf(FullQualifiedName(general_person_key)))
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(involed_in), FullQualifiedName(person_fire_minutes))
    }

    override fun getSql(): String {
        return "${DataTables.quote(fire_minutes)}[1] / ${DataTables.quote(num_of_people)}[1]"
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}

class DispatchInvolvedInPersonEMSMinutesProcessor: GraphProcessor, AssociationProcessor() {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(
                        FullQualifiedName(ems_minutes),
                        FullQualifiedName(num_of_people)),
                FullQualifiedName(involed_in) to setOf(FullQualifiedName(involed_in_type)),
                FullQualifiedName(general_person) to setOf(FullQualifiedName(general_person_key)))
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(involed_in), FullQualifiedName(person_ems_minutes))
    }

    override fun getSql(): String {
        return "${DataTables.quote(ems_minutes)}[1] / ${DataTables.quote(num_of_people)}[1]"
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}



/* ************** Count of people/officers/units : on event ************* */

//@Component
class DispatchInvolvedInNumberOfPoliceProcessor: GraphProcessor, AssociationProcessor() {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_key)),
                FullQualifiedName(involed_in) to setOf(FullQualifiedName(involed_in_type)),
                FullQualifiedName(general_person) to setOf(FullQualifiedName(general_person_key)))
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(num_of_police_officers))
    }

    override fun getSql(): String {
        return countOf(general_person_key)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        //TODO: need valuefilter
        //return mapOf(FullQualifiedName(involed_in) to mapOf(FullQualifiedName(involed_in_type) to setOf(??????))
        return TODO()
    }
}

//@Component
class DispatchInvolvedInNumberOfPoliceNoDurationProcessor: GraphProcessor, AssociationProcessor() {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_key)),
                FullQualifiedName(involed_in) to setOf(FullQualifiedName(duration), FullQualifiedName(involed_in_type)),
                FullQualifiedName(general_person) to setOf(FullQualifiedName(general_person_key))) // not used for calculation, just for edges
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(num_of_police_officers_no_duration))
    }

    override fun getSql(): String {
        return emptyInvolvedInDurationCount()
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        //TODO: need valuefilter
        //return mapOf(FullQualifiedName(involed_in) to mapOf(FullQualifiedName(involed_in_type) to setOf(??????))
        return TODO()
    }
}


//@Component
class DispatchInvolvedInNumberOfPoliceUnitsProcessor: GraphProcessor, AssociationProcessor() {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_key)),
                FullQualifiedName(involed_in) to setOf(FullQualifiedName(duration)), // not used for calculation, just for edges
                FullQualifiedName(police_unit) to setOf(FullQualifiedName(police_unit_name)))
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(num_of_police_units))
    }

    override fun getSql(): String {
        return countOf(police_unit_name)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}

//@Component
class DispatchInvolvedInNumberOfPoliceUnitsNoDurationProcessor: GraphProcessor, AssociationProcessor() {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_key)),
                FullQualifiedName(involed_in) to setOf(FullQualifiedName(duration)),
                FullQualifiedName(police_unit) to setOf(FullQualifiedName(police_unit_name))) // not used for calculation, just for edges
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(num_of_police_units_no_duration))
    }

    override fun getSql(): String {
        return emptyInvolvedInDurationCount()
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}


//@Component
class DispatchInvolvedInNumberOfFireUnitsProcessor: GraphProcessor, AssociationProcessor() {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_key)),
                FullQualifiedName(involed_in) to setOf(FullQualifiedName(duration)), // not used for calculation, just for edges
                FullQualifiedName(fire_unit) to setOf(FullQualifiedName(fire_unit_name)))
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(num_of_fire))
    }

    override fun getSql(): String {
        return countOf(fire_unit_name)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}

//@Component
class DispatchInvolvedInNumberOfFireUnitsNoDurationProcessor: GraphProcessor, AssociationProcessor() {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_key)),
                FullQualifiedName(involed_in) to setOf(FullQualifiedName(duration)),
                FullQualifiedName(fire_unit) to setOf(FullQualifiedName(fire_unit_name))) // not used for calculation, just for edges
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(num_of_fire_no_duration))
    }

    override fun getSql(): String {
        return emptyInvolvedInDurationCount()
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}


//@Component
class DispatchInvolvedInNumberOfEMSUnitsProcessor: GraphProcessor, AssociationProcessor() {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_key)),
                FullQualifiedName(involed_in) to setOf(FullQualifiedName(duration)), // not used for calculation, just for edges
                FullQualifiedName(ems_unit) to setOf(FullQualifiedName(ems_unit_name)))
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(num_of_ems))
    }

    override fun getSql(): String {
        return countOf(ems_unit_name)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}

//@Component
class DispatchInvolvedInNumberOfEMSUnitsNoDurationProcessor: GraphProcessor, AssociationProcessor() {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_key)),
                FullQualifiedName(involed_in) to setOf(FullQualifiedName(duration)),
                FullQualifiedName(ems_unit) to setOf(FullQualifiedName(ems_unit_name))) // not used for calculation, just for edges
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(num_of_ems_no_duration))
    }

    override fun getSql(): String {
        return emptyInvolvedInDurationCount()
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}


//@Component
class DispatchInvolvedInNumberOfPeopleProcessor: GraphProcessor, AssociationProcessor() {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(general_person) to setOf(FullQualifiedName(general_person_key)),
                FullQualifiedName(involed_in) to setOf(FullQualifiedName(involed_in_type)),
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_key)))
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(num_of_people))
    }

    override fun getSql(): String {
        return countOf(general_person_key)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return TODO("filter on involvedin") //involed_in_type
    }
}



/* ************** Duration per department calculation: on association ************* */

//@Component
class DispatchInvolvedInPoliceMinutesProcessor: GraphProcessor, AssociationProcessor() {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_duration)),
                FullQualifiedName(involed_in) to setOf(FullQualifiedName(duration)),
                FullQualifiedName(general_person) to setOf(FullQualifiedName(general_person_key)))
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(police_minutes))
    }

    override fun getSql(): String {
        return getAssociationDurationCalc(duration, dispatch_duration)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return TODO("need to filter on officers")
    }
}

//@Component
class DispatchInvolvedInPoliceUnitMinutesProcessor: GraphProcessor, AssociationProcessor() {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_duration)),
                FullQualifiedName(involed_in) to setOf(FullQualifiedName(duration)),
                FullQualifiedName(police_unit) to setOf(FullQualifiedName(police_unit_key)))
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(police_unit_minutes))
    }

    override fun getSql(): String {
        return getAssociationDurationCalc(duration, dispatch_duration)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}

//@Component
class DispatchInvolvedInFireMinutesProcessor: GraphProcessor, AssociationProcessor() {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_duration)),
                FullQualifiedName(involed_in) to setOf(FullQualifiedName(duration)),
                FullQualifiedName(fire_unit) to setOf(FullQualifiedName(fire_unit_key)))
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(fire_minutes))
    }

    override fun getSql(): String {
        return getAssociationDurationCalc(duration, dispatch_duration)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}

//@Component
class DispatchInvolvedInEMSMinutesProcessor: GraphProcessor, AssociationProcessor() {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_duration)),
                FullQualifiedName(involed_in) to setOf(FullQualifiedName(duration)),
                FullQualifiedName(ems_unit) to setOf(FullQualifiedName(ems_unit_key)))
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(ems_minutes))
    }

    override fun getSql(): String {
        return getAssociationDurationCalc(duration, dispatch_duration)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}

