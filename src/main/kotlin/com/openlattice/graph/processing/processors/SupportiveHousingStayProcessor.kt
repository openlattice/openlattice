package com.openlattice.graph.processing.processors


import com.openlattice.postgres.DataTables
import org.springframework.stereotype.Component
import java.time.temporal.ChronoUnit


private const val entity_type = "housing.stay"
private const val start = "date.admission"
private const val end = "ol.datetime_release"
private const val duration = "housing.lengthofstay"


//@Component
class SupportiveHousingDurationProcessor: DurationProcessor() {

    override fun getSql(): String {
        val firstStart = firstStart()
        val lastEnd = lastEnd()
        return "SUM($lastEnd - " +
                "make_date(DATE_PART('year', $firstStart ), DATE_PART('month', $firstStart), DATE_PART('day', $firstStart)))"
    }

    override fun getHandledEntityType(): String {
        return entity_type
    }

    override fun getPropertyTypeForStart(): String {
        return start
    }

    override fun getPropertyTypeForEnd(): String {
        return end
    }

    override fun getPropertyTypeForDuration(): String {
        return duration
    }

    override fun getCalculationTimeUnit(): ChronoUnit {
        return ChronoUnit.DAYS
    }

    override fun getDisplayTimeUnit(): ChronoUnit {
        return ChronoUnit.DAYS
    }
}

//@Component
class SupportiveHousingEndDateProcessor: EndDateProcessor() {

    override fun getSql(): String {
        return "MAX((${addDurationToFirstStart()} * interval '1 hour')::date)"
    }

    override fun getHandledEntityType(): String {
        return entity_type
    }

    override fun getPropertyTypeForStart(): String {
        return start
    }

    override fun getPropertyTypeForEnd(): String {
        return end
    }

    override fun getPropertyTypeForDuration(): String {
        return duration
    }

    override fun getCalculationTimeUnit(): ChronoUnit {
        return ChronoUnit.DAYS
    }

    override fun getDisplayTimeUnit(): ChronoUnit {
        return ChronoUnit.DAYS
    }
}