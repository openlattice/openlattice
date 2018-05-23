/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.data.storage

import com.google.common.collect.SetMultimap
import com.google.common.hash.Hashing
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.DataTables.*
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.time.OffsetDateTime
import java.util.*
import java.util.function.Function
import java.util.function.Supplier

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
const val MAX_PREV_VERSION = "max_prev_version"
const val EXPANDED_VERSIONS = "expanded_versions"
const val BATCH_SIZE = 10000
private val logger = LoggerFactory.getLogger(PostgresEntityDataQueryService::class.java)

class PostgresEntityDataQueryService(private val hds: HikariDataSource) {
    fun streamableEntitySet(entitySetId: UUID,
                            authorizedPropertyTypes: Set<PropertyType>,
                            metadataOptions: Set<MetadataOption>,
                            version: Optional<Long> = Optional.empty()): PostgresIterable<SetMultimap<FullQualifiedName, Any>> {
        return streamableEntitySet(entitySetId, setOf(), authorizedPropertyTypes, metadataOptions, version)
    }

    fun streamableEntitySet(entitySetId: UUID,
                            entityKeyIds: Set<UUID>,
                            authorizedPropertyTypes: Set<PropertyType>,
                            metadataOptions: Set<MetadataOption>,
                            version: Optional<Long> = Optional.empty()): PostgresIterable<SetMultimap<FullQualifiedName, Any>> {
        return PostgresIterable(Supplier<StatementHolder> {
            val connection = hds.getConnection()
            val statement = connection.createStatement()
            val rs = statement.executeQuery(
                    if (version.isPresent) {
                        selectEntitySetWithPropertyTypesAndVersion(
                                entitySetId,
                                entityKeyIds,
                                authorizedPropertyTypes.map { it.id to it.type.fullQualifiedNameAsString }.toMap(),
                                metadataOptions,
                                version.get())
                    } else {
                        selectEntitySetWithPropertyTypes(
                                entitySetId,
                                entityKeyIds,
                                authorizedPropertyTypes.map { it.id to it.type.fullQualifiedNameAsString }.toMap(),
                                metadataOptions)
                    })
            StatementHolder(connection, statement, rs)
        }, Function<ResultSet, SetMultimap<FullQualifiedName, Any>> { ResultSetAdapters.implicitEntity(it, authorizedPropertyTypes, metadataOptions) })
    }


    fun upsertEntities(entitySetId: UUID, entities: Map<UUID, SetMultimap<UUID, Object>>, authorizedPropertyTypes: Set<PropertyType>, version: Long) {
        val connection = hds.getConnection()
        val statement = connection.createStatement()
        val entitySetPreparedStatement = connection.prepareStatement(upsertEntity(entitySetId))
        val preparedStatements = authorizedPropertyTypes
                .map { it.id to connection.prepareStatement(upsertPropertyValues(entitySetId, it.id, it.type.fullQualifiedNameAsString, version)) }
                .toMap()

        entities.forEach {
            entitySetPreparedStatement.setObject(1, it.key)
            entitySetPreparedStatement.addBatch()

            preparedStatements.forEach {
                it.value.setObject(
            }
        }

        val entitySetResults = entitySetPreparedStatement.executeBatch()

    }
}

fun upsertEntity(entitySetId: UUID, version: Long): String {
    val esTableName = DataTables.quote(DataTables.entityTableName(entitySetId))
    val columns = setOf(
            ID_VALUE.name,
            VERSION.name,
            LAST_WRITE.name,
            LAST_INDEX.name)
    return "INSERT INTO $esTableName (${columns.joinToString(",")}) VALUES( ?,$version,now(),${OffsetDateTime.MIN}) " +
            "ON CONFLICT (${ID_VALUE.name}) DO UPDATE SET ${VERSION.name} = $version, ${LAST_WRITE.name} = now() "
}

fun upsertPropertyValues(entitySetId: UUID, propertyTypeId: UUID, propertyType: String, version: Long): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))
    val columns = setOf(
            ENTITY_SET_ID.name,
            ID_VALUE.name,
            HASH.name,
            quote(propertyType),
            VERSION.name,
            VERSIONS.name,
            LAST_WRITE.name)

    //Insert new row or update version.
    return "INSERT INTO $propertyTable (${columns.joinToString(",")}) VALUES($entitySetId,?,?,?,?,?,?)" +
            "ON CONFLICT (${ENTITY_SET_ID.name},${ID_VALUE.name}, ${HASH.name}) DO UPDATE SET versions = versions || $version, version = $version "
}

fun selectEntitySetWithPropertyTypes(entitySetId: UUID,
                                     entityKeyIds: Set<UUID>,
                                     authorizedPropertyTypes: Map<UUID, String>,
                                     metadataOptions: Set<MetadataOption>): String {
    val esTableName = DataTables.quote(DataTables.entityTableName(entitySetId))
    //@formatter:off
    val columns = setOf(
            "${ID_VALUE.name}",
            if(metadataOptions.contains(MetadataOption.LAST_WRITE) ) {"${LAST_WRITE.name}" } else { "" },
            if(metadataOptions.contains(MetadataOption.LAST_INDEX) ) {"${LAST_INDEX.name}" } else { "" })
            .union( authorizedPropertyTypes.values.map(::quote ) )

    return "SELECT ${columns.filter(String::isNotBlank).joinToString (",")} \n" +
            "FROM $esTableName \n" +
            authorizedPropertyTypes
                    .map { "LEFT JOIN ${subSelectLatestVersionOfPropertyTypeInEntitySet(entitySetId, entityKeyIds, it.key, it.value )} USING (${ID.name} )" }
                    .joinToString("\n" )
    //@formatter:on
}

fun selectEntitySetWithPropertyTypesAndVersion(
        entitySetId: UUID,
        entityKeyIds: Set<UUID>,
        authorizedPropertyTypes: Map<UUID, String>,
        metadataOptions: Set<MetadataOption>,
        version: Long): String {
    val esTableName = DataTables.quote(DataTables.entityTableName(entitySetId))
    //@formatter:off
    val columns = setOf(
            "${ID_VALUE.name}",
            if(metadataOptions.contains(MetadataOption.LAST_WRITE) ) {"${LAST_WRITE.name}" } else { "" },
            if(metadataOptions.contains(MetadataOption.LAST_INDEX) ) {"${LAST_INDEX.name}" } else { "" })
            .union( authorizedPropertyTypes.values.map(::quote ) )

    return "SELECT ${columns.filter(String::isNotBlank).joinToString (",")} \n" +
            "FROM $esTableName \n" +
            authorizedPropertyTypes
                    .map { "LEFT JOIN ${selectVersionOfPropertyTypeInEntitySet(entitySetId, entityKeyIds, it.key, it.value, version )} USING (${ID.name} )" }
                    .joinToString("\n" )
    //@formatter:on
}

fun selectVersionOfPropertyTypeInEntitySet(entitySetId: UUID, entityKeyIds: Set<UUID>, propertyTypeId: UUID, fqn: String, version: Long): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))
    return "(SELECT ${ENTITY_SET_ID.name}, " +
            "   ${ID_VALUE.name}, " +
            "   ${DataTables.quote(fqn)}, " +
            "   $MAX_PREV_VERSION " +
            "FROM ${subSelectFilteredVersionOfPropertyTypeInEntitySet(entitySetId, entityKeyIds, propertyTypeId, fqn, version)}" +
            "WHERE ARRAY[$MAX_PREV_VERSION] <@ versions) as $propertyTable "
}

// We could combine latest and versioned reads, but it's easier to understand if they are separate.
fun subSelectLatestVersionOfPropertyTypeInEntitySet(entitySetId: UUID, entityKeyIds: Set<UUID>, propertyTypeId: UUID, fqn: String): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))
    val entityKeyIdsClause = "${ID_VALUE.name} IN (" + entityKeyIds.joinToString(",") + ")"
    return "(SELECT ${ENTITY_SET_ID.name}," +
            " ${ID_VALUE.name}," +
            " array_agg(${DataTables.quote(fqn)}) as ${DataTables.quote(fqn)}," +
            " ${VERSION.name} " +
            "FROM $propertyTable " +
            "WHERE ${ENTITY_SET_ID.name} = '$entitySetId' AND ${VERSION.name} >= 0 " +
            //@formatter:off
            if( !entityKeyIds.isEmpty() ) { " AND $entityKeyIdsClause" } else { "" } +
            //@formatter:on
            "GROUP BY (${ENTITY_SET_ID.name}, ${ID_VALUE.name}, ${HASH.name})) as $propertyTable "
}

fun subSelectFilteredVersionOfPropertyTypeInEntitySet(entitySetId: UUID, entityKeyIds: Set<UUID>, propertyTypeId: UUID, fqn: String, version: Long): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))
    val entityKeyIdsClause = "${ID_VALUE.name} IN (" + entityKeyIds.joinToString(",") + ")"
    return "(SELECT ${ENTITY_SET_ID.name}," +
            " ${ID_VALUE.name}, " +
            " ${HASH.name}, " +
            " array_agg(${DataTables.quote(fqn)}) as ${DataTables.quote(fqn)}," +
            " array_agg($EXPANDED_VERSIONS) as versions," +
            " max(abs($EXPANDED_VERSIONS)) as $MAX_PREV_VERSION " +
            "FROM $propertyTable, unnest(versions) as $EXPANDED_VERSIONS " +
            "WHERE ${ENTITY_SET_ID.name} = '$entitySetId' AND abs($EXPANDED_VERSIONS) <= $version " +
            //@formatter:off
            if( !entityKeyIds.isEmpty() ) { " AND $entityKeyIdsClause" } else { "" } +
            //@formatter:on
            "GROUP BY (${ENTITY_SET_ID.name}," +
            "   ${ID_VALUE.name}," +
            "   ${HASH.name})" +
            ") as $propertyTable "
}