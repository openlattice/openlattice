package com.openlattice.graph.processing.processors

import org.springframework.stereotype.Component
import java.time.temporal.ChronoUnit

private const val entity_type = "criminaljustice.incident"
private const val start = "incident.startdatetime"
private const val end = "incident.enddatetime"
private const val duration = "ol.durationhours"

@Component
class CriminalJusticeIncidentDurationProcessor:DurationProcessor() {

    override fun getSql(): String {
        val firstStart = sortedFirst(getPropertyTypeForStart())
        val lastEnd = sortedLast(getPropertyTypeForEnd())
        return "EXTRACT(epoch FROM ($lastEnd - $firstStart))/3600"
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
        return ChronoUnit.HOURS
    }
}

@Component
class CriminalJusticeIncidentEndDateProcessor:EndDateProcessor() {

    override fun getSql(): String {
        val firstStart = sortedFirst(getPropertyTypeForStart())
        return "$firstStart + ${getPropertyTypeForDuration()} * 60 * interval '1 minutes'"
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
        return ChronoUnit.HOURS
    }
}