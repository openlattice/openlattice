package com.openlattice.graph.processing.processors

import org.springframework.stereotype.Component
import java.time.temporal.ChronoUnit

private const val entity_type = "ol.dispatch"
private const val start = "time.alerted"
private const val end = "time.completed"
private const val duration = "ol.durationinterval"

@Component
class DispatchDurationProcessor: DurationProcessor() {

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


@Component
class DispatchEndDateProcessor: EndDateProcessor() {

    override fun getSql(): String {
        val firstStart = sortedFirst(getPropertyTypeForStart())
        return "$firstStart + ${getPropertyTypeForDuration()} * interval '1 minutes'"
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