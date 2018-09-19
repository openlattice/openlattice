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
class DispatchProcessor(edmManager: EdmManager, entityDataService: PostgresEntityDataQueryService):
        BaseDurationProcessor(edmManager, entityDataService) {

    private val handledEntityType = "ol.dispatch"

    companion object {
        private val logger = LoggerFactory.getLogger(DispatchProcessor.javaClass)
    }

    override fun processAssociations(newEntities: Map<UUID, Any?>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getLogger(): Logger {
        return logger
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

    override fun getTimeUnit(): ChronoUnit {
        return ChronoUnit.MINUTES
    }

    override fun getTransformationType(): String {
        return NONE
    }

    override fun handledEntityTypes(): Set<UUID> {
        return setOf(getEntityTypeId(handledEntityType))
    }

}