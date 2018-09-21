package com.openlattice.graph.processing.processors

import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.time.temporal.ChronoUnit


abstract class BaseDurationProcessor: GraphProcessor {

    protected abstract fun getHandledEntityType(): String
    protected abstract fun getPropertyTypeForStart(): String
    protected abstract fun getPropertyTypeForEnd(): String
    protected abstract fun getPropertyTypeForDuration(): String

    protected abstract fun getDisplayTimeUnit(): ChronoUnit
    protected abstract fun getCalculationTimeUnit(): ChronoUnit
}


abstract class DurationProcessor:BaseDurationProcessor() {
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