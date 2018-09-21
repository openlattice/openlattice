package com.openlattice.graph.processing.processors

import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.springframework.stereotype.Component
import java.time.temporal.ChronoUnit

@Component
class SupportiveHousingStayProcessor: DurationProcessor() {

    override fun getSql(): String {
        val firstStart = "${getPropertyTypeForStart()}[1]"
        val lastEnd = "${getPropertyTypeForEnd()}[array_length(${getPropertyTypeForEnd()}, 1)]"
        return "$lastEnd - make_date(DATE_PART('year', $firstStart ), DATE_PART('month', $firstStart), DATE_PART('day', $firstStart))"
    }

    override fun getHandledEntityType(): String {
        return "housing.stay"
    }

    override fun getPropertyTypeForStart(): String {
        return "date.admission"
    }

    override fun getPropertyTypeForEnd(): String {
        return "ol.datetime_release"
    }

    override fun getPropertyTypeForDuration(): String {
        return "housing.lengthofstay"
    }

    override fun getCalculationTimeUnit(): ChronoUnit {
        return ChronoUnit.DAYS
    }

    override fun getDisplayTimeUnit(): ChronoUnit {
        return ChronoUnit.DAYS
    }
}