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

import com.openlattice.analysis.RangeFilter
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.DataTables.*
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.util.*

/**
 * Handles reading data from linked entity sets.
 */
private val logger = LoggerFactory.getLogger(PostgresLinkedEntityDataQueryService::class.java)
const val ENTITIES_TABLE_ALIAS = "entity_key_ids"

private val entityKeyIdColumns = listOf(ENTITY_SET_ID.name, ID_VALUE.name).joinToString(",")

open class PostgresLinkedEntityDataQueryService(private val hds: HikariDataSource) {

}


/**
 *
 */
fun selectEntitySetWithPropertyTypesAndVersionSql(
        entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
        authorizedPropertyTypes: Map<UUID, String>,
        propertyTypeFilters: Map<UUID, Set<RangeFilter<Comparable<Any>>>>,
        version: Long,
        linked: Boolean,
        binaryPropertyTypes: Map<UUID, Boolean>
): String {
    val entitiesClause = buildEntitiesClause(entityKeyIds)

    val dataColumns =
            if (linked) {
                setOf(LINKING_ID.name)
            } else {
                setOf(ENTITY_SET_ID.name, ID_VALUE.name)
            }
                    .union(authorizedPropertyTypes.values.map(::quote))
                    .filter(String::isNotBlank)
                    .joinToString(",")

    val entitiesSubquerySql = buildSelectEntityKeyIdsFilteredByVersionSubquerySql(entitiesClause, version)

    val propertyTypeJoins = authorizedPropertyTypes
            .map {
                val subQuerySql = buildSelectVersionOfPropertyTypeInEntitySetSql(
                        entitiesClause,
                        it.key,
                        it.value,
                        version,
                        propertyTypeFilters[it.key] ?: setOf(),
                        linked,
                        binaryPropertyTypes[it.key]!!
                )
                "LEFT JOIN $subQuerySql USING (${ID.name}) "
            }
            .joinToString("\n")

    return "SELECT $dataColumns FROM ( $entitiesSubquerySql ) as $ENTITIES_TABLE_ALIAS $propertyTypeJoins"

}

/**
 * This routine generates SQL that performs the following steps:
 *
 * 1. Build the property table name, selection columns,
 * 2. Choose the right selection columns for this level.
 * 3. Prepare the property value aggregate fragment.
 * 4. Prepare the subquery fragment for retrieving entity key ids satisfying the provided clauses.
 *
 * @param entitiesClause The clause that controls which entities will be evaluated by this query.
 *
 * @return A SQL subquery statement that will return all
 */
internal fun buildSelectVersionOfPropertyTypeInEntitySetSql(
        entitiesClause: String,
        propertyTypeId: UUID,
        fqn: String,
        version: Long,
        filters: Set<RangeFilter<Comparable<Any>>>,
        linked: Boolean,
        binary: Boolean
): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))

    val selectColumns =
            if (linked) LINKING_ID.name
            else entityKeyIdColumns

    val arrayAgg = arrayAggSql(fqn, binary)

    val selectPropertyTypeIdsFilteredByVersion = selectPropertyTypeDataKeysFilteredByVersionSubquerySql(
            entitiesClause,
            buildFilterClause(fqn, filters),
            propertyTable,
            version
    )

    val linkingIdSubquerySql =
            if (linked) {
                "LEFT JOIN (SELECT $entityKeyIdColumns,${LINKING_ID.name} FROM ${IDS.name}) as linking_ids USING($entityKeyIdColumns)"
            } else {
                ""
            }

    return "(SELECT $selectColumns, $arrayAgg " +
            "FROM $selectPropertyTypeIdsFilteredByVersion " +
            linkingIdSubquerySql +
            "LEFT JOIN $propertyTable USING($entityKeyIdColumns) " +
            "GROUP BY($selectColumns)) as $propertyTable "
}

internal fun selectCurrentVersionOfPropertyTypeSql(
        entitiesClause: String,
        propertyTypeId: UUID,
        filters: Set<RangeFilter<Comparable<Any>>>,
        fqn: String,
        linked: Boolean,
        binary: Boolean
): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))

    val selectColumns =
            if (linked) LINKING_ID.name
            else entityKeyIdColumns

    val arrayAgg = arrayAggSql(fqn, binary)

    val filtersClause = buildFilterClause(fqn, filters)
    val linkingIdSubquerySql =
            if (linked) {
                "LEFT JOIN (SELECT $entityKeyIdColumns,${LINKING_ID.name} FROM ${IDS.name}) as linking_ids USING($entityKeyIdColumns)"
            } else {
                ""
            }

    return "(SELECT $selectColumns, $arrayAgg " +
            "FROM $propertyTable " +
            linkingIdSubquerySql +
            "WHERE ${VERSION.name} >= 0 $entitiesClause $filtersClause " +
            "GROUP BY ($selectColumns)) as $propertyTable "
}

/**
 * This routine generates SQL that performs the following steps:
 *
 * 1. Unnests the versions arrays for the given ids.
 * 2. Filters out any versions greater than the request version.
 * 3. Computes max and max absolute value of the remaining versions,
 * 4. Filters out any entity keys where the max and max absolute
 *    value are not identical, which will only happen when the
 *    greatest present version has been tombstoned for that property.
 *
 * @param entitiesClause The clause that controls which entities will be evaluated by this query.
 * @param filtersClause The clause that controls which filters will be applied when performing the read
 * @param propertyTable The property table to use for building the query.
 * @param version The version of the properties being read.
 *
 * @return A named SQL subquery fragment consisting of all entity key satisfying the provided clauses.
 */
internal fun selectPropertyTypeDataKeysFilteredByVersionSubquerySql(
        entitiesClause: String,
        filtersClause: String,
        propertyTable: String,
        version: Long
): String {
    return "(SELECT $entityKeyIdColumns " +
            "FROM ( SELECT $entityKeyIdColumns, max(versions) as abs_max, max(abs(versions)) as max_abs " +
            "       FROM (  SELECT $entityKeyIdColumns, unnest(versions) as versions " +
            "           FROM $propertyTable " +
            "           WHERE $entitiesClause $filtersClause) as $EXPANDED_VERSIONS " +
            "       WHERE abs(versions) <= $version " +
            "       GROUP BY($entityKeyIdColumns)) as unfiltered_data_keys " +
            "WHERE max_abs=abs_max ) as data_keys "

}


/**
 * This routine generates SQL that performs the following steps:
 *
 * 1. Unnests the versions arrays for the given ids.
 * 2. Filters out any versions greater than the request version.
 * 3. Computes max and max absolute value of the remaining versions,
 * 4. Filters out any entity keys where the max and max absolute
 *    value are not identical, which will only happen when the
 *    greatest present version has been tombstoned for that property.
 *
 * @param entitiesClause The clause that controls which entities will be evaluated by this query.
 * @param version The version of the properties being read.
 *
 * @return A named SQL subquery fragment consisting of all entity key satisfying the provided clauses from the
 * ids table.
 */
internal fun buildSelectEntityKeyIdsFilteredByVersionSubquerySql(
        entitiesClause: String,
        version: Long
): String {
    return "(SELECT $entityKeyIdColumns " +
            "FROM ( SELECT $entityKeyIdColumns, max(versions) as abs_max, max(abs(versions)) as max_abs " +
            "       FROM (  SELECT $entityKeyIdColumns, unnest(versions) as versions " +
            "           FROM ${ID.name} " +
            "           WHERE $entitiesClause ) as $EXPANDED_VERSIONS " +
            "       WHERE abs(versions) <= $version " +
            "       GROUP BY($entityKeyIdColumns)) as unfiltered_data_keys " +
            "WHERE max_abs=abs_max ) as data_keys "

}


internal fun arrayAggSql(fqn: String, binary: Boolean): String {
    return if (binary) " array_agg(encode(${DataTables.quote(fqn)}, 'base64')) as ${DataTables.quote(fqn)} "
    else " array_agg(${DataTables.quote(fqn)}) as ${DataTables.quote(fqn)} "
}

internal fun buildEntitiesClause(entityKeyIds: Map<UUID, Optional<Set<UUID>>>): String {
    return entityKeyIds.entries.joinToString(",") {
        val idsClause = it.value.map { " AND IN (" + it.joinToString(",") { it.toString() } + ")" }.orElse("")
        "(${ENTITY_SET_ID.name} = $it.key $idsClause)"
    }
}


internal fun buildFilterClause(fqn: String, filter: Set<RangeFilter<Comparable<Any>>>): String {
    return filter.joinToString(" AND ") { it.asSql(quote(fqn)) }
}
