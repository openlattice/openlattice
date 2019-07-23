package com.openlattice.postgres

import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.module.kotlin.readValue
import com.openlattice.IdConstants
import com.openlattice.data.Property
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.edm.EdmConstants.Companion.ID_FQN
import com.openlattice.edm.PostgresEdmTypeConverter
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.ResultSetAdapters.entitySetId
import com.openlattice.postgres.ResultSetAdapters.id
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.sql.*
import java.sql.Date
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.*


internal class PostgresResultSetAdapters

private val logger = LoggerFactory.getLogger(PostgresResultSetAdapters::class.java)
private val mapper = ObjectMappers.newJsonMapper()

private fun <T> getEntityPropertiesByFunctionResult(
        rs: ResultSet,
        authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
        byteBlobDataManager: ByteBlobDataManager,
        mapper: (PropertyType) -> T
): Pair<UUID, MutableMap<T, MutableSet<Any>>> {
    val id = id(rs)
    val entitySetId = entitySetId(rs)
    val data = mutableMapOf<T, MutableSet<Any>>()

    val propertyTypes = authorizedPropertyTypes.getValue(entitySetId).values

    for (propertyType in propertyTypes) {
        val objects = propertyValue(rs, propertyType)

        if (objects != null) {
            val key = mapper(propertyType)
            if (propertyType.datatype == EdmPrimitiveTypeKind.Binary) {
                data[key] = byteBlobDataManager.getObjects(objects as List<String>).toMutableSet()
            } else {
                data[key] = mutableSetOf<Any>(objects)
            }
        }
    }
    return id to data
}

@Throws(SQLException::class)
fun getJsonEntityPropertiesByPropertyTypeId(
        rs: ResultSet,
        authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
        byteBlobDataManager: ByteBlobDataManager
): Pair<UUID, MutableMap<UUID, MutableSet<Property>>> {
    val id = id(rs)
    val entitySetId = entitySetId(rs)
    val propertyTypes = authorizedPropertyTypes.getValue(entitySetId)
    return id to propertyTypes
            .map { PostgresEdmTypeConverter.map(it.value.datatype) }
            .mapNotNull { datatype ->
                rs.getString("v_$datatype")
            }
            .map { mapper.readValue<MutableMap<UUID, MutableSet<Property>>>(it) }
            .reduce { acc, mutableMap ->
                acc.putAll(mutableMap)
                return@reduce acc
            }
}

@Throws(SQLException::class)
fun getEntityPropertiesByPropertyTypeId2(
        rs: ResultSet,
        authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
        byteBlobDataManager: ByteBlobDataManager
): Pair<UUID, MutableMap<UUID, MutableSet<Any>>> {
    val id = id(rs)
    val entitySetId = entitySetId(rs)
    val propertyTypes = authorizedPropertyTypes.getValue(entitySetId)
    val propertyValues = propertyTypes
            .map { (_, propertyType) ->
                val datatype = PostgresEdmTypeConverter.map(propertyType.datatype)
                val json = rs.getString("v_$datatype")  //This will never be blank worst case it will return '{}'

                val dataMap = mapper
                        .readValue<MutableMap<UUID, MutableSet<Any>>>(json)
                (dataMap.keys - propertyTypes.keys).forEach { dataMap.remove(it) }

                if (propertyType.datatype == EdmPrimitiveTypeKind.Binary) {
                    val urls = dataMap.getOrElse(propertyType.id) { mutableSetOf() }
                    dataMap[propertyType.id] = byteBlobDataManager.getObjects(urls).toMutableSet()
                }
                dataMap
            }
            .fold(mutableMapOf(IdConstants.ID_ID.id to mutableSetOf<Any>(id))) { acc, mutableMap ->
                acc.putAll(mutableMap)
                return@fold acc
            }



    return id to propertyValues
}


@Throws(SQLException::class)
fun getEntityPropertiesByPropertyTypeId3(
        rs: ResultSet,
        authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
        byteBlobDataManager: ByteBlobDataManager
): Pair<UUID, MutableMap<FullQualifiedName, MutableSet<Any>>> {
    val id = id(rs)
    logger.info("This should get logged a bunch")
    val entitySetId = entitySetId(rs)
    val propertyTypes = authorizedPropertyTypes.getValue(entitySetId)
    val propertyValues = propertyTypes
            .map { (_, propertyType) ->
                val datatype = PostgresEdmTypeConverter.map(propertyType.datatype)
                val json = rs.getString("v_$datatype")

                val dataMap = mapper
                        .readValue<MutableMap<UUID, MutableSet<Any>>>(json)
                (dataMap.keys - propertyTypes.keys).forEach { dataMap.remove(it) }

                if (propertyType.datatype == EdmPrimitiveTypeKind.Binary) {
                    logger.info("This should never get logged")
                    val urls = dataMap.getOrElse(propertyType.id) { mutableSetOf() }
                    dataMap[propertyType.id] = byteBlobDataManager.getObjects(urls).toMutableSet()
                }

                dataMap.mapKeys { propertyTypes.getValue(it.key).type }.toMutableMap()
            }
            .fold(mutableMapOf(ID_FQN to mutableSetOf<Any>(id))) { acc, mutableMap ->
                acc.putAll(mutableMap)
                return@fold acc
            }

    return id to propertyValues

}

@Throws(SQLException::class)
fun getEntityPropertiesByPropertyTypeId(
        rs: ResultSet,
        authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
        byteBlobDataManager: ByteBlobDataManager
): Pair<UUID, MutableMap<UUID, MutableSet<Any>>> {
    return getEntityPropertiesByPropertyTypeId2(rs, authorizedPropertyTypes, byteBlobDataManager)
}

@Throws(SQLException::class)
fun getEntityPropertiesByFullQualifiedName(
        rs: ResultSet,
        authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
        byteBlobDataManager: ByteBlobDataManager
): Pair<UUID, MutableMap<FullQualifiedName, MutableSet<Any>>> {
    return getEntityPropertiesByPropertyTypeId3(
            rs, authorizedPropertyTypes, byteBlobDataManager
    )
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