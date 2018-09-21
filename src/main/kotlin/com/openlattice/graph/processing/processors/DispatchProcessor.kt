package com.openlattice.graph.processing.processors

import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.time.temporal.ChronoUnit

//@Component
class DispatchProcessor: DurationProcessor() {

    override fun getSql(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getHandledEntityType(): String {
        return "ol.dispatch"
    }

    override fun getPropertyTypeForStart(): String {
        return "time.alerted"
    }

    override fun getPropertyTypeForEnd(): String {
        return "time.completed"
    }

    override fun getPropertyTypeForDuration(): String {
        return "ol.durationinterval"
    }

    override fun getCalculationTimeUnit(): ChronoUnit {
        return ChronoUnit.MINUTES
    }

    override fun getDisplayTimeUnit(): ChronoUnit {
        return ChronoUnit.MINUTES
    }

}