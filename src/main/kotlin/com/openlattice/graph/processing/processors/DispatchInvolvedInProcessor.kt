package com.openlattice.graph.processing.processors

import com.openlattice.analysis.requests.ValueFilter
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
    return "SUM(CASE WHEN (ARRAY_LENGTH($edgeDurationFqn) IS null) THEN $eventDurationFqn[1] ELSE $edgeDurationFqn[1] END)"
}


// TODO handle cases also, when multiple associations are for same dispath-officer/unit duo (array_agg should do it)

/* ************** Base duration calculation : on association ************* */
//@Component
class DispatchInvolvedInProcessor: GraphProcessor  {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(FullQualifiedName(involed_in) to setOf(FullQualifiedName(start), FullQualifiedName(end)),
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_key)))
        // add ol.dispatch also into query, so that we only get rows "involedin  dispatch"
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(involed_in), FullQualifiedName(duration))
    }

    override fun getSql(): String {
        val firstStart = sortedFirst(start)
        val lastEnd = sortedLast(end)
        return numberOfMinutes(firstStart, lastEnd)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<Any>>> {
        return mapOf()
    }
}



/* ************** Per person, per unit duration calculation : on association ************* */

class DispatchInvolvedInPersonPoliceMinutesProcessor: GraphProcessor {
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
        return "($police_minutes + $police_unit_minutes) / $num_of_people"
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<Any>>> {
        //TODO: need valuefilter
        //return mapOf(FullQualifiedName(involed_in) to mapOf(FullQualifiedName(involed_in_type) to setOf(??????))
        return TODO("filter on involvedin type")
    }
}

class DispatchInvolvedInPersonFireMinutesProcessor: GraphProcessor {
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
        return "$fire_minutes / $num_of_people"
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<Any>>> {
        return mapOf()
    }
}

class DispatchInvolvedInPersonEMSMinutesProcessor: GraphProcessor {
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
        return "$ems_minutes / $num_of_people"
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<Any>>> {
        return mapOf()
    }
}



/* ************** Count of people/officers/units : on event ************* */

//@Component
class DispatchInvolvedInNumberOfPoliceProcessor: GraphProcessor {
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
        return "COUNT($general_person_key)"
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<Any>>> {
        //TODO: need valuefilter
        //return mapOf(FullQualifiedName(involed_in) to mapOf(FullQualifiedName(involed_in_type) to setOf(??????))
        return TODO()
    }
}

//@Component
class DispatchInvolvedInNumberOfPoliceNoDurationProcessor: GraphProcessor {
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
        return "COUNT(CASE WHEN (ARRAY_LENGTH($duration) IS null) THEN 1 END)" // array_length should return null if empty
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<Any>>> {
        //TODO: need valuefilter
        //return mapOf(FullQualifiedName(involed_in) to mapOf(FullQualifiedName(involed_in_type) to setOf(??????))
        return TODO()
    }
}


//@Component
class DispatchInvolvedInNumberOfPoliceUnitsProcessor: GraphProcessor {
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
        return "COUNT($police_unit_name)"
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<Any>>> {
        return mapOf()
    }
}

//@Component
class DispatchInvolvedInNumberOfPoliceUnitsNoDurationProcessor: GraphProcessor {
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
        return "COUNT(CASE WHEN (ARRAY_LENGTH($duration) IS null) THEN 1 END)" // array_length should return null if empty
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<Any>>> {
        return mapOf() //TODO: duration in (null) ???
    }
}


//@Component
class DispatchInvolvedInNumberOfFireUnitsProcessor: GraphProcessor {
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
        return "COUNT($fire_unit_name)"
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<Any>>> {
        return mapOf()
    }
}

//@Component
class DispatchInvolvedInNumberOfFireUnitsNoDurationProcessor: GraphProcessor {
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
        return "COUNT(CASE WHEN (ARRAY_LENGTH($duration) IS null) THEN 1 END)" // array_length should return null if empty
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<Any>>> {
        return mapOf() //TODO: duration in (null) ???
    }
}


//@Component
class DispatchInvolvedInNumberOfEMSUnitsProcessor: GraphProcessor {
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
        return "COUNT($ems_unit_name)"
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<Any>>> {
        return mapOf()
    }
}

//@Component
class DispatchInvolvedInNumberOfEMSUnitsNoDurationProcessor: GraphProcessor {
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
        return "COUNT(CASE WHEN (ARRAY_LENGTH($duration) IS null) THEN 1 END)" // array_length should return null if empty
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<Any>>> {
        return mapOf() //TODO: duration in (null) ???
    }
}


//@Component
class DispatchInvolvedInNumberOfPeopleProcessor: GraphProcessor {
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
        return "COUNT($general_person_key)"
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<Any>>> {
        return TODO("filter on involvedin") //involed_in_type
    }
}



/* ************** Duration per department calculation: on association ************* */

//@Component
class DispatchInvolvedInPoliceMinutesProcessor: GraphProcessor {
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

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<Any>>> {
        return TODO("need to filter on officers")
    }
}

//@Component
class DispatchInvolvedInPoliceUnitMinutesProcessor: GraphProcessor {
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

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<Any>>> {
        return mapOf()
    }
}

//@Component
class DispatchInvolvedInFireMinutesProcessor: GraphProcessor {
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

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<Any>>> {
        return mapOf()
    }
}

//@Component
class DispatchInvolvedInEMSMinutesProcessor: GraphProcessor {
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

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<Any>>> {
        return mapOf()
    }
}

