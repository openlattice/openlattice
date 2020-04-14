package com.openlattice.postgres

import com.google.common.base.Preconditions
import com.openlattice.data.storage.BinaryDataWithContentType
import com.openlattice.edm.type.PropertyType
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.geo.Geospatial
import org.apache.olingo.commons.api.edm.geo.Point
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetDateTime
import java.util.*
import java.util.regex.Pattern
import java.util.stream.Collectors

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class JsonDeserializer {
    companion object {
        private val logger = LoggerFactory.getLogger(
                JsonDeserializer::class.java
        )
        private val decoder = Base64.getDecoder()
        private val geographyPointRegex = Pattern
                .compile("(-?[0-9]+\\.[0-9]+), *(-?[0-9]+\\.[0-9]+)")

        @JvmStatic
        fun validateFormatAndNormalize(
                propertyValues: Map<UUID, Set<Any>>,
                authorizedPropertiesWithDataType: Map<UUID, PropertyType>
        ): Map<UUID, Set<Any>> {
            return validateFormatAndNormalize(
                    propertyValues,
                    authorizedPropertiesWithDataType
            ) { "No additional info." }
        }

        @JvmStatic
        fun validateFormatAndNormalize(
                propertyValues: Map<UUID, Set<Any>>,
                authorizedPropertiesWithDataType: Map<UUID, PropertyType>,
                lazyMessage: () -> String
        ): Map<UUID, Set<Any>> {
            val normalizedPropertyValues: MutableMap<UUID, MutableSet<Any>> = mutableMapOf()
            for ((propertyTypeId, valueSet) in propertyValues) {
                try {
                    val propertyType = authorizedPropertiesWithDataType[propertyTypeId]
                    val dataType = propertyType!!.datatype
                    //If enum values are specified for this property type, make sure only valid enum values have been
                    //provided for parsing.
                    if (propertyType.enumValues.isNotEmpty()) {
                        val enumValues = propertyType.enumValues
                        val invalidEnumValues = valueSet.stream()
                                .filter { value: Any? -> !enumValues.contains(value) }
                                .collect(
                                        Collectors.toList()
                                )
                        if (invalidEnumValues.isNotEmpty()) {
                            val errMsg = ("Received invalid enum values " + invalidEnumValues.toString()
                                    + " for property type "
                                    + propertyType.type.fullQualifiedNameAsString)
                            logger.error(errMsg)
                            throw IllegalStateException(errMsg)
                        }
                    }


                    if (valueSet == null || valueSet.isEmpty()) {
                        normalizedPropertyValues.getOrPut(propertyTypeId) { mutableSetOf() }
                        continue
                    }

                    for (value in valueSet) {
                        val normalizedValue = validateFormatAndNormalize(dataType, propertyTypeId, value)
                        if (normalizedValue != null) {
                            normalizedPropertyValues
                                    .getOrPut(propertyTypeId) { mutableSetOf() }
                                    .add(normalizedValue)
                        } else {
                            logger.error(
                                    "Skipping null value when normalizing data {} for property type {}: {}",
                                    valueSet,
                                    propertyTypeId,
                                    lazyMessage()
                            )
                        }
                    }
                } catch (e: Exception) {
                    throw IllegalStateException(
                            "Unable to write to property type $propertyTypeId with values " + valueSet
                                    .toString() + ": " + lazyMessage(), e
                    )
                }
            }
            return normalizedPropertyValues
        }

        @SuppressFBWarnings(value = ["SF_SWITCH_FALLTHROUGH"], justification = "by design")
        private fun validateFormatAndNormalize(
                dataType: EdmPrimitiveTypeKind,
                propertyTypeId: UUID,
                value: Any?
        ): Any? {
            if (value == null) {
                return null
            }
            if (dataType == null) {
                logger.error("received a null datatype for property type {}", propertyTypeId.toString())
                throw NullPointerException(propertyTypeId.toString())
            }
            when (dataType) {
                EdmPrimitiveTypeKind.Boolean -> {
                    if (value is Boolean) {
                        return value
                    }
                    Preconditions.checkState(
                            value is String,
                            "Expected string for property type %s with property type %s, received %s",
                            dataType,
                            propertyTypeId,
                            value.javaClass
                    )
                    return java.lang.Boolean.valueOf(value as String?)
                }
                EdmPrimitiveTypeKind.Binary -> {
                    Preconditions.checkState(
                            value is Map<*, *>,
                            "Expected map for property type %s with property type %s, received %s",
                            dataType,
                            propertyTypeId,
                            value.javaClass
                    )
                    val valuePair = value as Map<String, Any>
                    val contentType = valuePair["content-type"]
                    val data = valuePair["data"]
                    Preconditions.checkState(
                            contentType is String,
                            "Expected string for content type, received %s",
                            contentType!!.javaClass
                    )
                    Preconditions.checkState(
                            data is String,
                            "Expected string for binary data, received %s",
                            data!!.javaClass
                    )
                    return BinaryDataWithContentType((contentType as String?)!!, decoder.decode(data as String?))
                }
                EdmPrimitiveTypeKind.Date -> {
                    Preconditions.checkState(
                            value is String,
                            "Expected string for property type %s with data %s,  received %s",
                            dataType,
                            propertyTypeId,
                            value.javaClass
                    )
                    return LocalDate.parse(value as String?)
                }
                EdmPrimitiveTypeKind.DateTimeOffset -> {
                    if (value is OffsetDateTime) {
                        return value
                    }
                    Preconditions.checkState(
                            value is String,
                            "Expected string for property type %s with data %s,  received %s",
                            dataType,
                            propertyTypeId,
                            value.javaClass
                    )
                    return OffsetDateTime.parse(value as String?)
                }
                EdmPrimitiveTypeKind.Duration -> {
                    Preconditions.checkState(
                            value is String,
                            "Expected string for property type %s with data %s,  received %s",
                            dataType,
                            propertyTypeId,
                            value.javaClass
                    )
                    return Duration.parse(value as String?).toMillis()
                }
                EdmPrimitiveTypeKind.Guid -> {
                    if (value is UUID) {
                        return value
                    }
                    Preconditions.checkState(
                            value is String,
                            "Expected string for property type %s with data %s,  received %s",
                            dataType,
                            propertyTypeId,
                            value.javaClass
                    )
                    return UUID.fromString(value as String?)
                }
                EdmPrimitiveTypeKind.String -> {
                    Preconditions.checkState(
                            value is String,
                            "Expected string for property type %s with data %s,  received %s",
                            dataType,
                            propertyTypeId,
                            value.javaClass
                    )
                    return value
                }
                EdmPrimitiveTypeKind.TimeOfDay -> {
                    Preconditions.checkState(
                            value is String,
                            "Expected string for property type %s with data %s,  received %s",
                            dataType,
                            propertyTypeId,
                            value.javaClass
                    )
                    return LocalTime.parse(value as String?)
                }
                EdmPrimitiveTypeKind.Decimal, EdmPrimitiveTypeKind.Double, EdmPrimitiveTypeKind.Single -> return value.toString().toDouble()
                EdmPrimitiveTypeKind.Byte, EdmPrimitiveTypeKind.SByte -> return value.toString().toByte()
                EdmPrimitiveTypeKind.Int16 -> return value.toString().toShort()
                EdmPrimitiveTypeKind.Int32 -> return value.toString().toInt()
                EdmPrimitiveTypeKind.Int64 -> return value.toString().toLong()
                EdmPrimitiveTypeKind.GeographyPoint -> if (value is LinkedHashMap<*, *>) {
                    // Raw data binding deserializes a pojo into linked hashmap :/
                    val point = value as LinkedHashMap<String, Any>
                    if ("POINT" == point["geoType"] && "GEOGRAPHY" == point["dimension"]) {
                        // adhere to elasticsearch format of lat,lon:
                        // https://www.elastic.co/guide/en/elasticsearch/reference/current/geo-point.html
                        return point["y"].toString() + "," + point["x"]
                    }
                } else if (value is Point) {
                    val point = value
                    if (point.geoType == Geospatial.Type.POINT && point.dimension == Geospatial.Dimension.GEOGRAPHY) {
                        return point.y.toString() + "," + point.x
                    }
                } else if (value is String) {
                    val m = geographyPointRegex.matcher(value as String?)
                    if (m.matches()) {
                        return m.group(1) + "," + m.group(2)
                    }
                }
                else -> return value
            }
            throw IllegalArgumentException()
        }
    }
}