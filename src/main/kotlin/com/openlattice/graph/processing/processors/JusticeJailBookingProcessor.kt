package com.openlattice.graph.processing.processors

import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.datastore.services.EdmManager
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.temporal.ChronoUnit
import java.util.*

//@Component
class JusticeJailBookingProcessor(edmManager: EdmManager, entityDataService: PostgresEntityDataQueryService):
        BaseDurationProcessor(edmManager, entityDataService) {
    override fun isEndDateBased(): Boolean {
        TODO()
    }

    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getSql(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    private val handledEntityType = "justice.JailBooking"

    companion object {
        private val logger = LoggerFactory.getLogger(JusticeJailBookingProcessor::class.java)
    }

    override fun getLogger(): Logger {
        return logger
    }

    override fun getPropertyTypeForStart(): String {
        return "publicsafety.datebooked"
    }

    override fun getPropertyTypeForEnd(): String {
        return "ol.datetime_released"
    }

    override fun getPropertyTypeForDuration(): String {
        return "ol.durationdays"
    }

    override fun getCalculationTimeUnit(): ChronoUnit {
        return ChronoUnit.HOURS
    }

    override fun getDisplayTimeUnit(): ChronoUnit {
        return ChronoUnit.DAYS
    }
}