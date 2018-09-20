package com.openlattice.graph.processing.processors

import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.datastore.services.EdmManager
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.temporal.ChronoUnit

class InvolvedInDispatchProcessor(edmManager: EdmManager, entityDataService: PostgresEntityDataQueryService):
        BaseDurationProcessor(edmManager, entityDataService)  {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getSql(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    private val handledEntityType = "ol.involvedin"

    companion object {
        private val logger = LoggerFactory.getLogger(InvolvedInDispatchProcessor.javaClass)
    }

    override fun getLogger(): Logger {
        return logger
    }

    override fun isEndDateBased():Boolean {
        return false
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