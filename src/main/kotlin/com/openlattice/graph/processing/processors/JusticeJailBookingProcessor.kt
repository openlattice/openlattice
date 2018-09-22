package com.openlattice.graph.processing.processors

import org.springframework.stereotype.Component
import java.time.temporal.ChronoUnit

private const val entity_type = "justice.JailBooking"
private const val start = "publicsafety.datebooked"
private const val end = "ol.datetime_released"
private const val duration = "ol.durationdays"

@Component
class JusticeJailBookingDurationProcessor: DurationProcessor() {

    override fun getSql(): String {
        val firstStart = sortedFirst(getPropertyTypeForStart())
        val lastEnd = sortedLast(getPropertyTypeForEnd())
        return "EXTRACT(epoch FROM ($lastEnd - $firstStart))/3600/24"
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
        return ChronoUnit.HOURS
    }

    override fun getDisplayTimeUnit(): ChronoUnit {
        return ChronoUnit.DAYS
    }
}

@Component
class JusticeJailBookingEndDateProcessor: EndDateProcessor() {

    override fun getSql(): String {
        val firstStart = sortedFirst(getPropertyTypeForStart())
        return "$firstStart + ${getPropertyTypeForDuration()} * 24 * interval '1 hour'"
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
        return ChronoUnit.HOURS
    }

    override fun getDisplayTimeUnit(): ChronoUnit {
        return ChronoUnit.DAYS
    }
}