package com.openlattice.graph.processing.processors

import org.springframework.stereotype.Component
import java.time.temporal.ChronoUnit

private const val entity_type = "ol.involvedin"
private const val start = "ol.datetimestart"
private const val end = "ol.datetimeend"
private const val duration = "ol.organizationtime"

@Component
class InvolvedInDispatchDurationProcessor: DurationProcessor()  {

    override fun getSql(): String {
        val firstStart = sortedFirst(getPropertyTypeForStart())
        val lastEnd = sortedLast(getPropertyTypeForEnd())
        return numberOfMinutes(firstStart, lastEnd)
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
        return ChronoUnit.MINUTES
    }

    override fun getDisplayTimeUnit(): ChronoUnit {
        return ChronoUnit.MINUTES
    }
}