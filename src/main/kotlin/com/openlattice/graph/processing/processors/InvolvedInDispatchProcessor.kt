package com.openlattice.graph.processing.processors

import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.time.temporal.ChronoUnit

//@Component
class InvolvedInDispatchProcessor: DurationProcessor()  {

    override fun getSql(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getHandledEntityType(): String {
        return "ol.involvedin"
    }

    override fun getPropertyTypeForStart(): String {
        return "ol.datetimestart"
    }

    override fun getPropertyTypeForEnd(): String {
        return "ol.datetimeend"
    }

    override fun getPropertyTypeForDuration(): String {
        return "ol.organizationtime"
    }

    override fun getCalculationTimeUnit(): ChronoUnit {
        return ChronoUnit.MINUTES
    }

    override fun getDisplayTimeUnit(): ChronoUnit {
        return ChronoUnit.MINUTES
    }
}