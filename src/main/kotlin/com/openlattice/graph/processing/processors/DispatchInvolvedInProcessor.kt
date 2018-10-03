package com.openlattice.graph.processing.processors

import com.openlattice.analysis.requests.ValueFilter
import com.openlattice.postgres.DataTables
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.springframework.stereotype.Component

private const val involved_in = "ol.involvedin"
private const val involved_in_key = "ol.id"
private const val involved_in_role = "ol.role"

private const val person_police_minutes = "ol.personpoliceminutes"
private const val person_fire_minutes = "ol.personfireminutes"
private const val person_ems_minutes = "ol.personemsminutes"


private const val general_person = "general.person"
private const val general_person_key = "nc.SubjectIdentification"


private const val dispatch = "ol.dispatch"
private const val dispatch_key = "ol.id"
private const val dispatch_duration = "ol.durationinterval"

private const val involvedin_start = "ol.datetimestart"
private const val involvedin_end = "ol.datetimeend"
private const val involvedin_duration = "ol.organizationtime"

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
private const val fire_minutes = "ol.firedeptminutes"
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


private fun getUnitMinutesOnEventCalc(edgeDuration:String, eventDuration:String, numOfUnitNoDuration: String):String {
    return "( SUM(${DataTables.quote(edgeDuration)}[1]) + ( $numOfUnitNoDuration[1] * ${averageOf(eventDuration)} ) )"
}

private fun countOf(columnName: String): String {
    return "COUNT(${DataTables.quote(columnName)})"
}

private fun averageOf(columnName: String): String {
    return "AVG(${DataTables.quote(columnName)}[1])"
}

private fun noDurationCount(sumNumberColumn: String): String {
    return "( ${averageOf(sumNumberColumn)} - ${countOf(involvedin_duration)} )"
}

private fun proportionOf(numerators: List<String>, denominator: String): String {
    return "SUM( (${numerators.joinToString( separator = "\"[1] + \"", prefix = "\"", postfix = "\"[1] ")} }) / ${DataTables.quote(denominator)}[1] )"
}

private fun officerFilters(): ValueFilter<String> {
    return ValueFilter(setOf("'Officer'"))
}


/* ************** Base duration calculation : on association ************* */
@Component
class DispatchInvolvedInProcessor: AssociationProcessor  {
    override fun getSrcInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf()
    }

    override fun getDstInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_key)))
    }

    override fun getEdgeInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(FullQualifiedName(involved_in) to setOf(FullQualifiedName(involvedin_start), FullQualifiedName(involvedin_end)))
    }

    override fun getOutput(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(involved_in), FullQualifiedName(involvedin_duration))
    }

    override fun getSql(): String {
        val firstStart = "(SELECT unnest(${DataTables.quote(involvedin_start)}) ORDER BY 1 LIMIT 1)"
        val lastEnd = "(SELECT unnest(${DataTables.quote(involvedin_end)}) ORDER BY 1 DESC LIMIT 1)"
        return "SUM(EXTRACT(epoch FROM ($lastEnd - $firstStart))/60)"
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}



/* ************** Per person, per unit duration calculation : on association ************* */

/*
@Component
class DispatchInvolvedInPersonPoliceMinutesProcessor: AssociationProcessor {
    override fun getSrcInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(general_person) to setOf(FullQualifiedName(general_person_key)))
    }

    override fun getEdgeInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(involved_in) to setOf(FullQualifiedName(involved_in_role)))
    }

    override fun getDstInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(
                        FullQualifiedName(police_minutes),
                        FullQualifiedName(police_unit_minutes),
                        FullQualifiedName(num_of_people)))
    }

    override fun getOutput(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(involved_in), FullQualifiedName(person_police_minutes))
    }

    override fun getSql(): String {
        return "(${DataTables.quote(police_minutes)}[1] + ${DataTables.quote(police_unit_minutes)}[1]) " +
                "/ ${DataTables.quote(num_of_people)}[1]"
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        mapOf(FullQualifiedName(involved_in_role) )
    }
}*/ //TODO: filter only people(civils)

/*@Component
class DispatchInvolvedInPersonFireMinutesProcessor: AssociationProcessor {
    override fun getSrcInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(general_person) to setOf(FullQualifiedName(general_person_key)))
    }

    override fun getEdgeInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(involved_in) to setOf(FullQualifiedName(involved_in_key)))
    }

    override fun getDstInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(fire_minutes), FullQualifiedName(num_of_people)))
    }

    override fun getOutput(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(involved_in), FullQualifiedName(person_fire_minutes))
    }

    override fun getSql(): String {
        return proportionOf(listOf(fire_minutes), num_of_people)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}*/ // TODO filter only civils

/*@Component
class DispatchInvolvedInPersonEMSMinutesProcessor: AssociationProcessor {
    override fun getSrcInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(general_person) to setOf(FullQualifiedName(general_person_key)))
    }

    override fun getEdgeInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(involved_in) to setOf(FullQualifiedName(involved_in_key)))
    }

    override fun getDstInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(ems_minutes), FullQualifiedName(num_of_people)))
    }

    override fun getOutput(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(involved_in), FullQualifiedName(person_ems_minutes))
    }

    override fun getSql(): String {
        return proportionOf(listOf(ems_minutes), num_of_people)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}*/ // TODO filter only civils



/* ************** Count of people/officers/units : on event ************* */

@Component
class DispatchInvolvedInNumberOfPoliceProcessor: AssociationProcessor {
    override fun getSrcInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(general_person) to setOf(FullQualifiedName(general_person_key)))
    }

    override fun getEdgeInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(involved_in) to setOf(FullQualifiedName(involved_in_role)))
    }

    override fun getDstInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_key)))
    }

    override fun getOutput(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(num_of_police_officers))
    }

    override fun getSql(): String {
        return countOf(general_person_key)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf(FullQualifiedName(involved_in) to
                mapOf(FullQualifiedName(involved_in_role) to officerFilters()))
    }
}

@Component
class DispatchInvolvedInNumberOfPoliceNoDurationProcessor: AssociationProcessor {
    override fun getSrcInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(general_person) to setOf(FullQualifiedName(general_person_key))) // not used for calculation, just for edges
    }

    override fun getEdgeInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(involved_in) to setOf(FullQualifiedName(involvedin_duration), FullQualifiedName(involved_in_role)))
    }

    override fun getDstInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(num_of_police_officers)))
    }

    override fun getOutput(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(num_of_police_officers_no_duration))
    }

    override fun getSql(): String {
        return noDurationCount(num_of_police_officers)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf(FullQualifiedName(involved_in) to
                mapOf(FullQualifiedName(involved_in_role) to officerFilters()))
    }
}


@Component
class DispatchInvolvedInNumberOfPoliceUnitsProcessor: AssociationProcessor {
    override fun getSrcInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(police_unit) to setOf(FullQualifiedName(police_unit_name)))
    }

    override fun getEdgeInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(involved_in) to setOf(FullQualifiedName(involved_in_role))) // not used for calculation, just for edges
    }

    override fun getDstInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_key))) // not used for calculation, just for edges
    }

    override fun getOutput(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(num_of_police_units))
    }

    override fun getSql(): String {
        return countOf(police_unit_name)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}

@Component
class DispatchInvolvedInNumberOfPoliceUnitsNoDurationProcessor: AssociationProcessor {
    override fun getSrcInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(police_unit) to setOf(FullQualifiedName(police_unit_name))) // not used for calculation, just for edges
    }

    override fun getEdgeInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(involved_in) to setOf(FullQualifiedName(involvedin_duration)))
    }

    override fun getDstInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(num_of_police_units))) // not used for calculation, just for edges
    }

    override fun getOutput(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(num_of_police_units_no_duration))
    }

    override fun getSql(): String {
        return noDurationCount(num_of_police_units)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}


@Component
class DispatchInvolvedInNumberOfFireUnitsProcessor: AssociationProcessor {
    override fun getSrcInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(fire_unit) to setOf(FullQualifiedName(fire_unit_name)))
    }

    override fun getEdgeInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(involved_in) to setOf(FullQualifiedName(involved_in_role))) // not used for calculation, just for edges
    }

    override fun getDstInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_key))) // not used for calculation, just for edges
    }

    override fun getOutput(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(num_of_fire))
    }

    override fun getSql(): String {
        return countOf(fire_unit_name)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}

@Component
class DispatchInvolvedInNumberOfFireUnitsNoDurationProcessor: AssociationProcessor {
    override fun getSrcInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(fire_unit) to setOf(FullQualifiedName(fire_unit_name)))
    }

    override fun getEdgeInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(involved_in) to setOf(FullQualifiedName(involvedin_duration)))
    }

    override fun getDstInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(num_of_fire)))
    }

    override fun getOutput(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(num_of_fire_no_duration))
    }

    override fun getSql(): String {
        return noDurationCount(num_of_fire)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}


@Component
class DispatchInvolvedInNumberOfEMSUnitsProcessor: AssociationProcessor {
    override fun getSrcInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(ems_unit) to setOf(FullQualifiedName(ems_unit_name)))
    }

    override fun getEdgeInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(involved_in) to setOf(FullQualifiedName(involved_in_role))) // not used for calculation, just for edges
    }

    override fun getDstInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_key))) // not used for calculation, just for edges
    }

    override fun getOutput(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(num_of_ems))
    }

    override fun getSql(): String {
        return countOf(ems_unit_name)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}

@Component
class DispatchInvolvedInNumberOfEMSUnitsNoDurationProcessor: AssociationProcessor {
    override fun getSrcInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(ems_unit) to setOf(FullQualifiedName(ems_unit_name)))
    }

    override fun getEdgeInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(involved_in) to setOf(FullQualifiedName(involvedin_duration))) // not used for calculation, just for edges
    }

    override fun getDstInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(num_of_ems))) // not used for calculation, just for edges
    }

    override fun getOutput(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(num_of_ems_no_duration))
    }

    override fun getSql(): String {
        return noDurationCount(num_of_ems)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}


/*@Component
class DispatchInvolvedInNumberOfPeopleProcessor: AssociationProcessor {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(general_person) to setOf(FullQualifiedName(general_person_key)),
                FullQualifiedName(involved_in) to setOf(FullQualifiedName(involved_in_type)),
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_key)))
    }

    override fun getOutput(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(num_of_people))
    }

    override fun getSql(): String {
        return countOf(general_person_key)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return TODO("filter on involvedin") only civils
    }
}*/



/* ************** Duration per department calculation: on association ************* */

@Component
class DispatchInvolvedInPoliceMinutesProcessor: AssociationProcessor {
    override fun getSrcInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(general_person) to setOf(FullQualifiedName(general_person_key))) // not used for calculation, just for edges
    }

    override fun getEdgeInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(involved_in) to setOf(FullQualifiedName(involved_in_role), FullQualifiedName(involvedin_duration)))
    }

    override fun getDstInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_duration), FullQualifiedName(num_of_police_officers_no_duration)))
    }

    override fun getOutput(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(police_minutes))
    }

    override fun getSql(): String {
        return getUnitMinutesOnEventCalc(involvedin_duration, dispatch_duration, num_of_police_officers_no_duration)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf(FullQualifiedName(involved_in) to mapOf(FullQualifiedName(involved_in_role) to officerFilters()))
    }
}

@Component
class DispatchInvolvedInPoliceUnitMinutesProcessor: AssociationProcessor {
    override fun getSrcInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(police_unit) to setOf(FullQualifiedName(police_unit_key))) // not used for calculation, just for edges
    }

    override fun getEdgeInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(involved_in) to setOf(FullQualifiedName(involvedin_duration)))
    }

    override fun getDstInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_duration), FullQualifiedName(num_of_police_units_no_duration)))
    }

    override fun getOutput(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(police_unit_minutes))
    }

    override fun getSql(): String {
        return getUnitMinutesOnEventCalc(involvedin_duration, dispatch_duration, num_of_police_units_no_duration)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}

@Component
class DispatchInvolvedInFireMinutesProcessor: AssociationProcessor {
    override fun getSrcInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(fire_unit) to setOf(FullQualifiedName(fire_unit_key))) // not used for calculation, just for edges
    }

    override fun getEdgeInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(involved_in) to setOf(FullQualifiedName(involvedin_duration)))
    }

    override fun getDstInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(
                FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_duration), FullQualifiedName(num_of_fire_no_duration)))
    }

    override fun getOutput(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(fire_minutes))
    }

    override fun getSql(): String {
        return getUnitMinutesOnEventCalc(involvedin_duration, dispatch_duration, num_of_fire_no_duration)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}

@Component
class DispatchInvolvedInEMSMinutesProcessor: AssociationProcessor {
    override fun getSrcInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(FullQualifiedName(ems_unit) to setOf(FullQualifiedName(ems_unit_key))) // not used for calculation, just for edges
    }

    override fun getEdgeInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(FullQualifiedName(involved_in) to setOf(FullQualifiedName(involvedin_duration)))
    }

    override fun getDstInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(FullQualifiedName(dispatch) to setOf(FullQualifiedName(dispatch_duration), FullQualifiedName(num_of_ems_no_duration)))
    }

    override fun getOutput(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(dispatch), FullQualifiedName(ems_minutes))
    }

    override fun getSql(): String {
        return getUnitMinutesOnEventCalc(involvedin_duration, dispatch_duration, num_of_ems_no_duration)
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}

