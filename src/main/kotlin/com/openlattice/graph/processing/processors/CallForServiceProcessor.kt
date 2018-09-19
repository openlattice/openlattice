package com.openlattice.graph.processing.processors

import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.datastore.services.EdmManager
import com.openlattice.graph.core.GraphService
import com.openlattice.graph.processing.util.DurationTransformation
import com.openlattice.graph.processing.util.NONE
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.temporal.ChronoUnit
import java.util.*

@Component
class CallForServiceProcessor(private val graphService: GraphService, edmManager: EdmManager, entityDataService: PostgresEntityDataQueryService):
        BaseDurationProcessor(edmManager, entityDataService)  {

    private val involvedInAssociation = "ol.involvedin"
    private val involvedInProperty = "ol.personpoliceminutes"


    override fun processAssociations(newEntities: Map<UUID, Any?>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
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
    }

    private val handledEntityType = "publicsafety.callforservice"

    companion object {
        private val logger = LoggerFactory.getLogger(CallForServiceProcessor.javaClass)
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