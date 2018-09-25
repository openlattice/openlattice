package com.openlattice.graph.processing.processors

import com.openlattice.analysis.requests.ValueFilter
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.time.temporal.ChronoUnit


fun sortedFirst(arrayColumn: String):String {
    return "(SELECT unnest($arrayColumn) ORDER BY 1 LIMIT 1)"
}

fun sortedLast(arrayColumn: String):String {
    return "(SELECT unnest($arrayColumn) ORDER BY 1 DESC LIMIT 1)"
}

fun numberOfDays(start: String, end:String):String {
    return "(EXTRACT(epoch FROM ($start - $end))/3600/24)::integer"
}

fun numberOfMinutes(start: String, end:String):String {
    return "(EXTRACT(epoch FROM ($start - $end))/60)::integer"
}

abstract class BaseDurationProcessor: GraphProcessor {
    protected abstract fun getHandledEntityType(): String
    protected abstract fun getPropertyTypeForStart(): String
    protected abstract fun getPropertyTypeForEnd(): String
    protected abstract fun getPropertyTypeForDuration(): String

    protected abstract fun getDisplayTimeUnit(): ChronoUnit
    protected abstract fun getCalculationTimeUnit(): ChronoUnit

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<Any>>> {
        return mapOf()
    }
}


abstract class DurationProcessor:BaseDurationProcessor()  {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(FullQualifiedName(getHandledEntityType()) to
                setOf(FullQualifiedName(getPropertyTypeForStart()), FullQualifiedName(getPropertyTypeForEnd())))
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(getHandledEntityType()), FullQualifiedName(getPropertyTypeForDuration()))
    }
}

abstract class EndDateProcessor:BaseDurationProcessor(), GraphProcessor {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(FullQualifiedName(getHandledEntityType()) to
                setOf(FullQualifiedName(getPropertyTypeForStart()), FullQualifiedName(getPropertyTypeForDuration())))
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(getHandledEntityType()), FullQualifiedName(getPropertyTypeForEnd()))
    }
}