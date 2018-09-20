package com.openlattice.graph.processing.processors

import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.datastore.services.EdmManager
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.temporal.ChronoUnit
import java.util.*

@Component
class SupportiveHousingStayProcessor(edmManager: EdmManager, entityDataService: PostgresEntityDataQueryService):
        BaseDurationProcessor(edmManager, entityDataService) {

    override fun isEndDateBased(): Boolean {
        return true
    }

    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(FullQualifiedName(handledEntityType) to
                setOf(FullQualifiedName(getPropertyTypeForStart()), FullQualifiedName(getPropertyTypeForEnd())))
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(handledEntityType), FullQualifiedName(getPropertyTypeForDuration()))
    }

    override fun getSql(): String {
        return " "
    }

    private val handledEntityType = "housing.stay"

    companion object {
        private val logger = LoggerFactory.getLogger(SupportiveHousingStayProcessor.javaClass)
    }

    override fun getLogger(): Logger {
        return logger
    }

    override fun getPropertyTypeForStart(): String {
        return "date.admission"
    }

    override fun getPropertyTypeForEnd(): String {
        return "ol.datetime_release"
    }

    override fun getPropertyTypeForDuration(): String {
        return "housing.lengthofstay"
    }

    override fun getCalculationTimeUnit(): ChronoUnit {
        return ChronoUnit.DAYS
    }

    override fun getDisplayTimeUnit(): ChronoUnit {
        return ChronoUnit.DAYS
    }
}

@Component
class SupportiveHousingEndDateProcessor(edmManager: EdmManager, entityDataService: PostgresEntityDataQueryService):
        BaseDurationProcessor(edmManager, entityDataService) {

    override fun isEndDateBased(): Boolean {
        return true
    }

    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(FullQualifiedName(handledEntityType) to
                setOf(FullQualifiedName(getPropertyTypeForStart()), FullQualifiedName(getPropertyTypeForEnd())))
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(handledEntityType), FullQualifiedName(getPropertyTypeForDuration()))
    }

    override fun getSql(): String {
        return " "
    }

    private val handledEntityType = "housing.stay"

    companion object {
        private val logger = LoggerFactory.getLogger(SupportiveHousingStayProcessor.javaClass)
    }

    override fun getLogger(): Logger {
        return logger
    }

    override fun getPropertyTypeForStart(): String {
        return "date.admission"
    }

    override fun getPropertyTypeForEnd(): String {
        return "ol.datetime_release"
    }

    override fun getPropertyTypeForDuration(): String {
        return "housing.lengthofstay"
    }

    override fun getCalculationTimeUnit(): ChronoUnit {
        return ChronoUnit.DAYS
    }

    override fun getDisplayTimeUnit(): ChronoUnit {
        return ChronoUnit.DAYS
    }
}