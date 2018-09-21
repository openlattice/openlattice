package com.openlattice.graph.processing.processors

import org.springframework.stereotype.Component
import java.time.temporal.ChronoUnit

@Component
class SupportiveHousingEndDateProcessor: EndDateProcessor() {

    override fun getSql(): String {
        val firstStart = "\"${getPropertyTypeForStart()}\"[1]"
        return "($firstStart + \"${getPropertyTypeForDuration()}\"[1] * interval '1 hour')::date"
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