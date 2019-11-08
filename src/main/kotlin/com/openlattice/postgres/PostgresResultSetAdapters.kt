package com.openlattice.postgres

import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.module.kotlin.readValue
import com.openlattice.IdConstants
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.PROPERTIES
import com.openlattice.data.storage.VALUE
import com.openlattice.edm.EdmConstants.Companion.ID_FQN
import com.openlattice.edm.EdmConstants.Companion.LAST_WRITE_FQN
import com.openlattice.IdConstants.LAST_WRITE_ID
import com.openlattice.postgres.PostgresMetaDataProperties.LAST_WRITE
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.ResultSetAdapters.*
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

@Throws(SQLException::class)
fun getEntityPropertiesByPropertyTypeId(
        rs: ResultSet,
        authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
        metadataOptions: Set<MetadataOption>,
        byteBlobDataManager: ByteBlobDataManager
): Pair<UUID, MutableMap<UUID, MutableSet<Any>>> {
    val id = id(rs)
    val entitySetId = entitySetId(rs)
    val propertyTypes = authorizedPropertyTypes.getValue(entitySetId)

    val entity = readJsonDataColumns(
            rs,
            propertyTypes,
            byteBlobDataManager
    )
    // TODO Do we need ID column in properties?

    if (metadataOptions.contains(MetadataOption.LAST_WRITE)) {
        entity[LAST_WRITE_ID.id] = mutableSetOf<Any>(lastWriteTyped(rs))
    }

    return id to entity
}

/**
 * Returns linked entity data from the [ResultSet] mapped respectively by its id, entity set, origin id and property
 * type id.
 * Note: Do not include the linking id for the [IdConstants.ID_ID] key as a property for this adapter, because it is
 * used for linked entity indexing and we preserve that key for the origin id.
 * @see ConductorElasticsearchImpl.formatLinkedEntity
 */
@Throws(SQLException::class)
fun getEntityPropertiesByEntitySetIdOriginIdAndPropertyTypeId(
        rs: ResultSet,
        authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
        metadataOptions: Set<MetadataOption>,
        byteBlobDataManager: ByteBlobDataManager
): Pair<UUID, Pair<UUID, Map<UUID, MutableMap<UUID, MutableSet<Any>>>>> {
    val id = id(rs)
    val entitySetId = entitySetId(rs)
    val propertyTypes = authorizedPropertyTypes.getValue(entitySetId)

    val entities = readJsonDataColumnsWithId(rs, propertyTypes, byteBlobDataManager, metadataOptions)

    return id to (entitySetId to entities)
}

/**
 * Returns linked entity data from the [ResultSet] mapped respectively by its id, entity set, origin id and property
 * full qualified name.
 */
@Throws(SQLException::class)
fun getEntityPropertiesByEntitySetIdOriginIdAndPropertyTypeFqn(
        rs: ResultSet,
        authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
        metadataOptions: Set<MetadataOption>,
        byteBlobDataManager: ByteBlobDataManager
): Pair<UUID, Pair<UUID, Map<UUID, MutableMap<FullQualifiedName, MutableSet<Any>>>>> {
    val id = id(rs)
    val entitySetId = entitySetId(rs)
    val propertyTypes = authorizedPropertyTypes.getValue(entitySetId)

    val entities = readJsonDataColumnsWithId(rs, propertyTypes, byteBlobDataManager, metadataOptions)

    val entityByFqn = entities.mapValues { (_, propertyValues) ->
        propertyValues.mapKeys {
            if (it.key == LAST_WRITE_ID.id) {
                LAST_WRITE_FQN
            } else {
                propertyTypes.getValue(it.key).type
            }
        }.toMutableMap()
    }

    return id to (entitySetId to entityByFqn)
}

@Throws(SQLException::class)
fun getEntityPropertiesByFullQualifiedName(
        rs: ResultSet,
        authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
        metadataOptions: Set<MetadataOption>,
        byteBlobDataManager: ByteBlobDataManager
): Pair<UUID, MutableMap<FullQualifiedName, MutableSet<Any>>> {
    val id = id(rs)
    val entitySetId = entitySetId(rs)
    val propertyTypes = authorizedPropertyTypes.getValue(entitySetId)

    val entity = readJsonDataColumns(rs, propertyTypes, byteBlobDataManager)

    val entityByFqn = entity.mapKeys { propertyTypes.getValue(it.key).type }.toMutableMap()
    entityByFqn[ID_FQN] = mutableSetOf<Any>(id.toString())

    if (metadataOptions.contains(MetadataOption.LAST_WRITE)) {
        entityByFqn[LAST_WRITE_FQN] = mutableSetOf<Any>(lastWriteTyped(rs))
    }

    return id to entityByFqn
}

@Throws(SQLException::class)
fun readJsonDataColumns(
        rs: ResultSet,
        propertyTypes: Map<UUID, PropertyType>,
        byteBlobDataManager: ByteBlobDataManager
): MutableMap<UUID, MutableSet<Any>> {
    val entity = mapper.readValue<MutableMap<UUID, MutableSet<Any>>>(rs.getString(PROPERTIES))

    // Note: this call deletes all entries from result, which is not in propertyTypes (ID for example)
    (entity.keys - propertyTypes.keys).forEach { entity.remove(it) }

    propertyTypes.forEach { (_, propertyType) ->

        if (propertyType.datatype == EdmPrimitiveTypeKind.Binary) {
            val urls = entity.getOrElse(propertyType.id) { mutableSetOf() }
            if (urls.isNotEmpty()) {
                entity[propertyType.id] = byteBlobDataManager.getObjects(urls).toMutableSet()
            }
        }
    }

    return entity
}

@Throws(SQLException::class)
fun readJsonDataColumnsWithId(
        rs: ResultSet,
        propertyTypes: Map<UUID, PropertyType>,
        byteBlobDataManager: ByteBlobDataManager,
        metadataOptions: Set<MetadataOption>
): MutableMap<UUID, MutableMap<UUID, MutableSet<Any>>> {

    val detailedEntity = mapper.readValue<MutableMap<UUID, MutableSet<MutableMap<String, Any>>>>(rs.getString(PROPERTIES))
    // origin id -> property type id -> values
    val entities = mutableMapOf<UUID, MutableMap<UUID, MutableSet<Any>>>()
    detailedEntity.forEach { (propertyTypeId, details) ->
        // only select properties which are authorized
        if (propertyTypes.keys.contains(propertyTypeId)) {
            details.forEach { entityDetail ->
                val originId = UUID.fromString(entityDetail[PostgresColumn.ID_VALUE.name] as String)
                val propertyValue = entityDetail.getValue(VALUE)

                if (!entities.containsKey(originId)) {
                    entities[originId] = mutableMapOf(propertyTypeId to mutableSetOf(propertyValue))
                } else {
                    entities.getValue(originId)[propertyTypeId] = mutableSetOf(propertyValue)
                }

                if (metadataOptions.contains(MetadataOption.LAST_WRITE)) {
                    val lastWrite = entityDetail[LAST_WRITE.name] as OffsetDateTime
                    entities.getValue(originId)[LAST_WRITE_ID.id] = mutableSetOf<Any>(lastWrite)
                }
            }
        }
    }

    propertyTypes.forEach { (_, propertyType) ->
        if (propertyType.datatype == EdmPrimitiveTypeKind.Binary) {
            entities.forEach { (_, entity) ->
                val urls = entity.getOrElse(propertyType.id) { mutableSetOf() }
                if (urls.isNotEmpty()) {
                    entity[propertyType.id] = byteBlobDataManager.getObjects(urls).toMutableSet()
                }
            }
        }
    }

    return entities
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