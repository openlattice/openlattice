package com.openlattice.graph.processing.processors

import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.datastore.services.EdmManager
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.temporal.ChronoUnit
import java.util.*

@Component
class DispatchProcessor(edmManager: EdmManager, entityDataService: PostgresEntityDataQueryService):
        BaseDurationProcessor(edmManager, entityDataService) {

    private val handledEntityType = "ol.dispatch"

    companion object {
        private val logger = LoggerFactory.getLogger(DispatchProcessor.javaClass)
    }

    override fun getLogger(): Logger {
        return logger
    }

    override fun isEndDateBased():Boolean {
        return false
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


    override fun handledEntityTypes(): Set<UUID> {
        return setOf(getEntityTypeId(handledEntityType))
    }

}