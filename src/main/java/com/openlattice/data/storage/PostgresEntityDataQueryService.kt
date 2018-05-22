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
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.util.*
import java.util.function.Supplier

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
const val MAX_PREV_VERSION = "max_prev_version"
const val EXPANDED_VERSIONS = "expanded_versions"

private val logger = LoggerFactory.getLogger(PostgresEntityDataQueryService::class.java)

class PostgresEntityDataQueryService(private val hds: HikariDataSource) {
    fun streamableEntitySet(entitySetId: UUID, authorizedPropertyTypes: Set<PropertyType>, metadataOptions: Set<MetadataOption>): PostgresIterable<SetMultimap<FullQualifiedName, Any>> {
        return PostgresIterable(Supplier<StatementHolder> {
            val rs: ResultSet
            val connection: Connection
            val statement: Statement
            connection = hds.getConnection()
            statement = connection.createStatement()
            rs = statement
                    .executeQuery(selectEntitySetWithPropertyTypes(entitySetId, authorizedPropertyTypes.map { it.id to it.type.fullQualifiedNameAsString }.toMap(), metadataOptions))
            return StatementHolder(connection, statement, rs)
        }, fun(rs: ResultSet): SetMultimap<FullQualifiedName, Any> {
                return ResultSetAdapters.implicitEntity(rs, authorizedPropertyTypes)
        }).stream();

    }

}

fun selectEntitySetWithPropertyTypes(entitySetId: UUID, authorizedPropertyTypes: Map<UUID, String>, metadataOptions: Set<MetadataOption>): String {
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
                    .map { "LEFT JOIN ${subSelectLatestVersionOfPropertyTypeInEntitySet(entitySetId, it.key, it.value )} USING (${ID.name} )" }
                    .joinToString("\n" )
    //@formatter:on
}

fun selectEntitySetWithPropertyTypes(entitySetId: UUID, authorizedPropertyTypes: Map<UUID, String>, metadataOptions: Set<MetadataOption>, version: Long): String {
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
                    .map { "LEFT JOIN ${selectVersionOfPropertyTypeInEntitySet(entitySetId, it.key, it.value, version )} USING (${ID.name} )" }
                    .joinToString("\n" )
    //@formatter:on
}

fun selectVersionOfPropertyTypeInEntitySet(entitySetId: UUID, propertyTypeId: UUID, fqn: String, version: Long): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))
    return "(SELECT ${ENTITY_SET_ID.name}, " +
            "   ${ID_VALUE.name}, " +
            "   ${DataTables.quote(fqn)}, " +
            "   $MAX_PREV_VERSION " +
            "FROM ${subSelectFilteredVersionOfPropertyTypeInEntitySet(entitySetId, propertyTypeId, fqn, version)}" +
            "WHERE ARRAY[$MAX_PREV_VERSION] <@ versions) as $propertyTable "
}

fun subSelectLatestVersionOfPropertyTypeInEntitySet(entitySetId: UUID, propertyTypeId: UUID, fqn: String): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))
    return "(SELECT ${ENTITY_SET_ID.name}," +
            " ${ID_VALUE.name}," +
            " array_agg(${DataTables.quote(fqn)}) as ${DataTables.quote(fqn)}," +
            " ${VERSION.name} " +
            "FROM $propertyTable " +
            "WHERE ${ENTITY_SET_ID.name} = '$entitySetId' AND ${VERSION.name} >= 0 " +
            "GROUP BY (${ENTITY_SET_ID.name}, ${ID_VALUE.name}, ${HASH.name})) as $propertyTable "
}

fun subSelectFilteredVersionOfPropertyTypeInEntitySet(entitySetId: UUID, propertyTypeId: UUID, fqn: String, version: Long): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))
    return "(SELECT ${ENTITY_SET_ID.name}," +
            " ${ID_VALUE.name}, " +
            " ${HASH.name}, " +
            " array_agg(${DataTables.quote(fqn)}) as ${DataTables.quote(fqn)}," +
            " array_agg($EXPANDED_VERSIONS) as versions," +
            " max(abs($EXPANDED_VERSIONS)) as $MAX_PREV_VERSION " +
            "FROM $propertyTable, unnest(versions) as $EXPANDED_VERSIONS " +
            "WHERE ${ENTITY_SET_ID.name} = '$entitySetId' AND abs($EXPANDED_VERSIONS) <= $version " +
            "GROUP BY (${ENTITY_SET_ID.name}," +
            "   ${ID_VALUE.name}," +
            "   ${HASH.name})" +
            ") as $propertyTable "
}