package com.openlattice.graph.processing.processors

import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.processing.util.DurationTransformation
import com.openlattice.graph.processing.util.TransformationFactory
import com.openlattice.postgres.JsonDeserializer
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.Logger
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import java.time.temporal.Temporal
import java.util.*


abstract class BaseDurationProcessor (
        protected val edmManager:EdmManager,
        private val entityDataService: PostgresEntityDataQueryService
): GraphProcessor {

    //TODO: refactor association handling
    protected val personEntityType = "general.person"

    private val logger = getLogger()

    protected abstract fun getLogger(): Logger

    override fun process( entities: Map<UUID, Map<UUID, Map<UUID, Set<Any>>>>, propagationStarted: OffsetDateTime ) {
        val fromPropertyType = edmManager.getPropertyType(FullQualifiedName(getPropertyTypeForStart()))
        val untilPropertyType = edmManager.getPropertyType(FullQualifiedName(getPropertyTypeForEnd()))
        val durationPropertyType = edmManager.getPropertyType(FullQualifiedName(getPropertyTypeForDuration()))

        val newEntities = mutableMapOf<UUID, Any?>()

        entities.forEach {
            val entitySetId = it.key

            it.value.forEach {
                var propagationUpdateTime = propagationStarted

                val entityKeyId = it.key

                val from = normalizeValue(it.value, fromPropertyType)
                val until = normalizeValue(it.value, untilPropertyType)
                val duration = normalizeValue(it.value, durationPropertyType)
                var newDuration = duration

                val timeUnit = getTimeUnit()

                when (from) {
                    is Temporal -> when (until) {
                        is Temporal -> { // start and end date(time) is present
                            val calculatedDuration = timeUnit.between(from, until)

                            val transformation = getTransformation()
                            val convertedDuration = when (transformation) {
                                null -> {
                                    calculatedDuration
                                }
                                else -> transformation.convertTo(calculatedDuration)
                            }

                            if (duration == null || duration != (convertedDuration)) {
                                newDuration = convertedDuration
                                updateEntity(convertedDuration, entitySetId, entityKeyId, durationPropertyType)
                                propagationUpdateTime = OffsetDateTime.now()

                                logger.info("from: $from, to: $until, old duration: $duration, new duration: $convertedDuration")
                            }
                        }
                        null -> {
                            val newUntil = when (duration) { // start and duration is present
                                is Int -> from.plus(duration.toLong(), timeUnit)

                                is Long -> from.plus(duration, timeUnit)
                                is Double -> {
                                    val transformation = getTransformation()
                                    val convertedDuration = when (transformation) {
                                        null -> {
                                            logger.error("No transformation on duration with double type, converting it to long")
                                            duration.toLong()
                                        }
                                        else -> transformation.convertFrom(duration)
                                    }

                                    from.plus(convertedDuration, timeUnit)
                                }
                                null -> {
                                    logger.error("Both ${getPropertyTypeForEnd()} and ${getPropertyTypeForDuration()} value for entity set $it was null!")
                                    null
                                }
                                else -> {
                                    logger.error("${getPropertyTypeForDuration()} for entity set $it was not of type Int, Long or Double!")
                                    null
                                }
                            }

                            if(newUntil != null) {
                                updateEntity(newUntil, entitySetId, entityKeyId, untilPropertyType)
                                propagationUpdateTime = OffsetDateTime.now()
                                logger.info("from: $from, duration: $duration old to: $until, new to : $newUntil")
                            }
                        }
                        else -> {
                            logger.error("${getPropertyTypeForEnd()} for entity set $it was not of type Temporal!")
                        }
                    }
                    null -> {
                        logger.error("${getPropertyTypeForStart()} for entity set $it was null!")
                    }
                    else -> {
                        logger.error("${getPropertyTypeForStart()} for entity set $it was not of type Temporal!")
                    }
                }

                entityDataService.markAsProcessed(entitySetId, setOf(it.key), propagationUpdateTime)
                newEntities[it.key] = newDuration
            }
        }
        processAssociations(newEntities)
    }

    private fun normalizeValue(properties: Map<UUID, Set<Any>>, propertyType: PropertyType):Any? {
        val firstValue = properties[propertyType.id]?.firstOrNull()

        return JsonDeserializer.validateFormatAndNormalize (
                propertyType.datatype,
                propertyType.id,
                firstValue)
    }

    protected fun updateEntity(newValue: Any, entitySetId: UUID, entityKeyId: UUID, propertyType: PropertyType) {
        entityDataService.replaceEntities(
                entitySetId,
                mapOf(entityKeyId to mapOf(propertyType.id to setOf(newValue))),
                mapOf(propertyType.id to propertyType))
    }

    protected abstract fun getPropertyTypeForStart(): String
    protected abstract fun getPropertyTypeForEnd(): String
    protected abstract fun getPropertyTypeForDuration(): String

    protected abstract fun getTimeUnit(): ChronoUnit
    protected abstract fun getTransformationType(): String

    protected abstract fun processAssociations(newEntities: Map<UUID, Any?>)

    private fun getTransformation():DurationTransformation? {
        return TransformationFactory.getTransformationFor(getTransformationType())
    }

    protected fun getEntityTypeId(fqn:String):UUID {
        return edmManager.getEntityType(FullQualifiedName(fqn)).id
    }
}