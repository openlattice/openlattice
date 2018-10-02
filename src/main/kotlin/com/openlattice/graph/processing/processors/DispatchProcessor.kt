package com.openlattice.graph.processing.processors

import com.openlattice.analysis.requests.ValueFilter
import com.openlattice.postgres.DataTables
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.springframework.stereotype.Component
import java.time.temporal.ChronoUnit

private const val entity_type = "ol.dispatch"
private const val start = "datetime.alerted"
private const val end = "date.completeddatetime"
private const val duration = "ol.durationinterval"

@Component
class DispatchDurationProcessor: DurationProcessor() {

    override fun getSql(): String {
        return numberOfMinutes()
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

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }
}
