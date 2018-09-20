package com.openlattice.graph.processing.processors

import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.processing.processors.util.DurationCalculator
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.Logger
import java.lang.IllegalStateException
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import java.time.temporal.Temporal
import java.util.*


abstract class BaseDurationProcessor (
        protected val edmManager:EdmManager,
        private val entityDataService: PostgresEntityDataQueryService
): GraphProcessor {

    private val logger = getLogger()

    protected abstract fun getLogger(): Logger

    override fun process( entities: Map<UUID, Map<UUID, Map<UUID, Set<Any>>>>, propagationStarted: OffsetDateTime ) {
        val fromPropertyType = edmManager.getPropertyType(FullQualifiedName(getPropertyTypeForStart()))
        val untilPropertyType = edmManager.getPropertyType(FullQualifiedName(getPropertyTypeForEnd()))
        val durationPropertyType = edmManager.getPropertyType(FullQualifiedName(getPropertyTypeForDuration()))

        entities.forEach {
            val entitySetId = it.key

            it.value.forEach {
                try{
                    val duration = getDurationFor(it.value, fromPropertyType, untilPropertyType, durationPropertyType)

                    if(duration.isEndDateMissing()) {
                        updateEntity(duration.getCalculatedEndDate(), entitySetId, it.key, untilPropertyType)

                        entityDataService.markAsProcessed(entitySetId, setOf(it.key), OffsetDateTime.now())
                        logger.info("End date(time) updated: from: ${duration.from}, duration: ${duration.originalDuration}, to: ${duration.getCalculatedEndDate()}")
                    } else {
                        if (!duration.isCalculatedEqualToOriginal()) {
                            updateEntity(duration.getDisplayDuration(), entitySetId, it.key, durationPropertyType)

                            entityDataService.markAsProcessed(entitySetId, setOf(it.key), OffsetDateTime.now())
                            logger.info("Duration updated: from: ${duration.from}, to: ${duration.until}, old duration: ${duration.originalDuration}, new duration: ${duration.getDisplayDuration()}")
                        } else {

                            entityDataService.markAsProcessed(entitySetId, setOf(it.key), propagationStarted)
                        }
                    }

                } catch (e: IllegalStateException) {
                    logger.error("Skipping processing of entity ${it.key}: ${e.message}")
                }

            }
        }
    }

    private fun getDurationFor(properties: Map<UUID, Set<Any>>,
                                fromPropertyType: PropertyType,
                                untilPropertyType: PropertyType,
                                durationPropertyType: PropertyType): DurationCalculator {
        val froms = getValueFor(properties, fromPropertyType)
        val untils = getValueFor(properties, untilPropertyType)
        val durations = getValueFor(properties, durationPropertyType)

        if(froms == null) {
            throw IllegalStateException("Start date(time) should not be null for ${getPropertyTypeForDuration()}")
        }

        if(untils == null && durations == null) {
            throw IllegalStateException("Both end(time) and duration is null for ${getPropertyTypeForDuration()}")
        }

        if(durations != null && durations.size > 1) {
            throw IllegalStateException("Found more than one durations for ${getPropertyTypeForDuration()}")
        }

        val duration = if(durations == null) null else durations.first() as Number
        val until = if(untils == null) null else untils.map{ it as Temporal }.sortedWith(TemporalComparator()).last()

        return DurationCalculator(
                froms.map { it as Temporal }.sortedWith(TemporalComparator()).first(),
                until,
                duration,
                getCalculationTimeUnit(),
                getDisplayTimeUnit()
        )
    }

    private fun getValueFor(properties: Map<UUID, Set<Any>>, propertyType: PropertyType):Set<Any>? {
        return properties[propertyType.id]
    }

    private fun updateEntity(newValue: Any, entitySetId: UUID, entityKeyId: UUID, propertyType: PropertyType) {
        entityDataService.replaceEntities(
                entitySetId,
                mapOf(entityKeyId to mapOf(propertyType.id to setOf(newValue))),
                mapOf(propertyType.id to propertyType))
    }

    protected abstract fun getPropertyTypeForStart(): String
    protected abstract fun getPropertyTypeForEnd(): String
    protected abstract fun getPropertyTypeForDuration(): String

    protected abstract fun getDisplayTimeUnit(): ChronoUnit
    protected abstract fun getCalculationTimeUnit(): ChronoUnit

    protected abstract fun isEndDateBased(): Boolean


    protected fun getEntityTypeId(fqn:String):UUID {
        return edmManager.getEntityType(FullQualifiedName(fqn)).id
    }

    inner class TemporalComparator : Comparator<Temporal> {
        override fun compare(x: Temporal, y: Temporal): Int {
            return if(x is LocalDate) {
                x.compareTo(y as LocalDate)
            } else {
                (x as OffsetDateTime).compareTo(y as OffsetDateTime)
            }
        }
    }
}