package com.openlattice.graph.processing.processors

import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.time.temporal.ChronoUnit

//@Component
class JusticeBookingProcessor: DurationProcessor() {

    override fun getSql(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getHandledEntityType(): String {
        return "justice.booking"
    }

    override fun getPropertyTypeForStart(): String {
        return "date.booking"
    }

    override fun getPropertyTypeForEnd(): String {
        return "publicsafety.ReleaseDate"
    }

    override fun getPropertyTypeForDuration(): String {
        return "criminaljustice.timeserveddays"
    }

    override fun getCalculationTimeUnit(): ChronoUnit {
        return ChronoUnit.DAYS
    }

    override fun getDisplayTimeUnit(): ChronoUnit {
        return ChronoUnit.DAYS
    }

}