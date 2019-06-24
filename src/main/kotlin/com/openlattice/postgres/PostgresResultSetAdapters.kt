package com.openlattice.postgres

import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.edm.type.PropertyType
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import java.sql.*
import java.sql.Date
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.*


internal class PostgresResultSetAdapters

private val logger = LoggerFactory.getLogger(PostgresResultSetAdapters::class.java)

@Throws(SQLException::class)
fun implicitEntityValuesById(
        rs: ResultSet,
        authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
        byteBlobDataManager: ByteBlobDataManager
): Map<UUID, Set<Any>> {
    val data = HashMap<UUID, Set<Any>>()

    val allPropertyTypes = authorizedPropertyTypes.values
            .flatMap { propertyTypesOfEntitySet -> propertyTypesOfEntitySet.values }
            .toSet()

    for (propertyType in allPropertyTypes) {
        val objects = propertyValue(rs, propertyType)

        if (objects != null) {
            if (propertyType.datatype == EdmPrimitiveTypeKind.Binary) {
                data[propertyType.id] = mutableSetOf(byteBlobDataManager.getObjects(objects as List<String>))
            } else {
                data[propertyType.id] = mutableSetOf(objects)
            }
        }
    }

    return data
}

//TODO: If we are getting NPEs on read we may have to do better filtering here.
@Throws(SQLException::class)
private fun propertyValue(rs: ResultSet, propertyType: PropertyType): List<*>? {
    val fqn = propertyType.type.fullQualifiedNameAsString

    val arr = rs.getArray(fqn)
    return if (arr != null) {
        when (propertyType.datatype) {
            EdmPrimitiveTypeKind.String, EdmPrimitiveTypeKind.GeographyPoint -> (arr.array as Array<String>).toList()
            EdmPrimitiveTypeKind.Guid -> (arr.array as Array<UUID>).toList()
            EdmPrimitiveTypeKind.Byte -> rs.getBytes(fqn)?.toList()
            EdmPrimitiveTypeKind.Int16 -> (arr.array as Array<Short>).toList()
            EdmPrimitiveTypeKind.Int32 -> (arr.array as Array<Int>).toList()
            EdmPrimitiveTypeKind.Duration, EdmPrimitiveTypeKind.Int64 -> (arr.array as Array<Long>).toList()
            EdmPrimitiveTypeKind.Date -> (arr.array as Array<Date>).map { it.toLocalDate() }
            EdmPrimitiveTypeKind.TimeOfDay -> (arr.array as Array<Time>).map { it.toLocalTime() }
            EdmPrimitiveTypeKind.DateTimeOffset -> (arr.array as Array<Timestamp>)
                    .map { ts ->
                        OffsetDateTime
                                .ofInstant(Instant.ofEpochMilli(ts.time), ZoneId.of("UTC"))
                    }
            EdmPrimitiveTypeKind.Double -> (arr.array as Array<Double>).toList()
            EdmPrimitiveTypeKind.Boolean -> (arr.array as Array<Boolean>).toList()
            EdmPrimitiveTypeKind.Binary -> (arr.array as Array<String>).toList()
            else -> {
                logger.error(
                        "Unable to read property type {}.",
                        propertyType.id
                )
                null
            }
        }
    } else {
        null
    }
}