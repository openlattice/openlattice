package com.openlattice.graph.processing.processors

import com.openlattice.analysis.requests.ValueFilter
import com.openlattice.postgres.DataTables
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.time.temporal.ChronoUnit




abstract class BaseDurationProcessor: GraphProcessor {
    protected abstract fun getHandledEntityType(): String
    protected abstract fun getPropertyTypeForStart(): String
    protected abstract fun getPropertyTypeForEnd(): String
    protected abstract fun getPropertyTypeForDuration(): String

    protected abstract fun getDisplayTimeUnit(): ChronoUnit
    protected abstract fun getCalculationTimeUnit(): ChronoUnit

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }

    override fun isSelf(): Boolean {
        return true
    }

    protected fun firstStart():String {
        return "(SELECT unnest(${DataTables.quote(getPropertyTypeForStart())}) ORDER BY 1 LIMIT 1)"
    }

    protected fun lastEnd(): String {
        return "(SELECT unnest(${DataTables.quote(getPropertyTypeForEnd())}) ORDER BY 1 DESC LIMIT 1)"
    }

    protected fun numberOfDays():String {
        return "SUM(EXTRACT(epoch FROM (${lastEnd()} - ${firstStart()}))/3600/24)"
    }

    protected fun numberOfHours():String {
        return "SUM(EXTRACT(epoch FROM (${lastEnd()} - ${firstStart()}))/3600)"
    }

    protected fun numberOfMinutes():String {
        return "SUM(EXTRACT(epoch FROM (${lastEnd()} - ${firstStart()}))/60)"
    }

    protected fun addDurationToFirstStart(): String {
        val firstStart = firstStart()
        return "$firstStart + ${DataTables.quote(getPropertyTypeForDuration())}[1] "
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

abstract class EndDateProcessor:BaseDurationProcessor() {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(FullQualifiedName(getHandledEntityType()) to
                setOf(FullQualifiedName(getPropertyTypeForStart()), FullQualifiedName(getPropertyTypeForDuration())))
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(getHandledEntityType()), FullQualifiedName(getPropertyTypeForEnd()))
    }
}