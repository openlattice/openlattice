package com.openlattice.graph.processing.processors

import org.springframework.stereotype.Component
import java.time.temporal.ChronoUnit

private const val entity_type = "justice.booking"
private const val start = "date.booking"
private const val end = "publicsafety.ReleaseDate"
private const val duration = "criminaljustice.timeserveddays"

@Component
class JusticeBookingDurationProcessor: DurationProcessor() {

    override fun getSql(): String {
        return numberOfDays()
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
