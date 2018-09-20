package com.openlattice.graph.processing.processors

import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.datastore.services.EdmManager
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.temporal.ChronoUnit
import java.util.*

@Component
class JusticeBookingProcessor(edmManager: EdmManager, entityDataService:PostgresEntityDataQueryService):
        BaseDurationProcessor(edmManager, entityDataService) {

    private val handledEntityType = "justice.booking"

    companion object {
        private val logger = LoggerFactory.getLogger(JusticeBookingProcessor.javaClass)
    }

    override fun getLogger(): Logger {
        return logger
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