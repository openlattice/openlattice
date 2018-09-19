package com.openlattice.graph.processing.processors

import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.datastore.services.EdmManager
import com.openlattice.graph.processing.util.NONE
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

    override fun processAssociations(newEntities: Map<UUID, Any?>) {
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

    override fun getTimeUnit(): ChronoUnit {
        return ChronoUnit.DAYS
    }

    override fun getTransformationType(): String {
        return NONE
    }

    override fun handledEntityTypes(): Set<UUID> {
        return setOf(getEntityTypeId(handledEntityType))
    }

}