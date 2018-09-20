package com.openlattice.graph.processing.processors

import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.datastore.services.EdmManager
import com.openlattice.graph.core.GraphService
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.temporal.ChronoUnit
import java.util.*

@Component
class CriminalJusticeIncidentProcessor(edmManager: EdmManager, entityDataService: PostgresEntityDataQueryService):
        BaseDurationProcessor(edmManager, entityDataService) {

    private val handledEntityType = "criminaljustice.incident"

    private val appearsInAssociation = "general.appearsin"
    private val appearsInProperty = "ol.personpoliceminutes"

    private val arrestedInAssociation = "criminaljustice.arrestedin"
    private val arrestedInProperty = "ol.personpoliceminutes"

    companion object {
        private val logger = LoggerFactory.getLogger(SupportiveHousingStayProcessor.javaClass)
    }

    override fun isEndDateBased():Boolean {
        return false
    }

    /*fun processAssociations(newEntities: Map<UUID, Any?>) {
        updateSimpleAssociation(newEntities, appearsInAssociation, appearsInProperty)
        updateSimpleAssociation(newEntities, arrestedInAssociation, arrestedInProperty)
    }

    private fun updateSimpleAssociation(newEntities: Map<UUID, Any?>, edgeTypeName:String, propertyTypeName: String) {
        val personEntitySetIds = edmManager.getEntitySetsOfType(edmManager.getEntityType(
                FullQualifiedName(personEntityType)).id).map { it.id }

        val edgeEntitySets = edmManager.getEntitySetsOfType(edmManager.getEntityType(
                FullQualifiedName(edgeTypeName)).id).map { it.id }

        // UUID of events (destination) with count of people
        val transformation = DurationTransformation(60)
        val perEventAssociations = graphService.getEntitiesForDestination(personEntitySetIds, edgeEntitySets, newEntities.keys)
                .groupBy {it.dst.entityKeyId}

        perEventAssociations.forEach {
            val sumVal = newEntities[it.key]
            when(sumVal) {
                is Double -> {
                    val minuteSumValue = transformation.convertFrom(sumVal)
                    val perPersonValue = minuteSumValue.toDouble()/it.value.size

                    it.value.forEach {
                        updateEntity(perPersonValue, it.edge.entitySetId, it.edge.entityKeyId, edmManager.getPropertyType(FullQualifiedName(propertyTypeName)))
                    }
                }
                else -> {
                    logger.error("Can't propagate values to association $edgeTypeName: duration value, $sumVal is not valid.")
                }
            }

        }
    }*/

    override fun getLogger(): Logger {
        return logger
    }

    override fun getPropertyTypeForStart(): String {
        return "incident.startdatetime"
    }

    override fun getPropertyTypeForEnd(): String {
        return "incident.enddatetime"
    }

    override fun getPropertyTypeForDuration(): String {
        return "ol.durationhours"
    }

    override fun getCalculationTimeUnit(): ChronoUnit {
        return ChronoUnit.MINUTES
    }

    override fun getDisplayTimeUnit(): ChronoUnit {
        return ChronoUnit.HOURS
    }

    override fun handledEntityTypes(): Set<UUID> {
        return setOf(getEntityTypeId(handledEntityType))
    }
}