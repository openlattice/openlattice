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

import com.openlattice.analysis.requests.Filter
import com.openlattice.postgres.DataTables.*
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.ResultSetAdapters
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.util.*

/**
 * Handles reading data from linked entity sets.
 */
private val logger = LoggerFactory.getLogger(PostgresLinkedEntityDataQueryService::class.java)
const val ENTITIES_TABLE_ALIAS = "entity_key_ids"
const val LEFT_JOIN = "LEFT JOIN"
const val INNER_JOIN = "INNER JOIN"
val entityKeyIdColumns = listOf(ENTITY_SET_ID.name, ID_VALUE.name).joinToString(",")

open class PostgresLinkedEntityDataQueryService(private val hds: HikariDataSource) {

}

/**
 *
 */
fun selectEntitySetWithCurrentVersionOfPropertyTypes(
        entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
        propertyTypes: Map<UUID, String>,
        returnedPropertyTypes: Collection<UUID>,
        authorizedPropertyTypes: Map<UUID, Set<UUID>>,
        propertyTypeFilters: Map<UUID, Set<Filter>>,
        metadataOptions: Set<MetadataOption>,
        linking: Boolean,
        binaryPropertyTypes: Map<UUID, Boolean>,
        metadataFilters: String = ""
): String {
    val entitiesClause = buildEntitiesClause(entityKeyIds, linking)
    val entitiesSubquerySql = selectEntityKeyIdsWithCurrentVersionSubquerySql(entitiesClause, metadataOptions, linking)

    val joinColumns =
            if (linking) {
                listOf(LINKING_ID.name)
            } else {
                listOf(ENTITY_SET_ID.name, ID_VALUE.name)
            }

    val dataColumns = joinColumns
            .union(metadataOptions.map(MetadataOption::name))
            .union(returnedPropertyTypes.map { propertyTypes[it]!! })
            .joinToString(",")

    val propertyTableJoins =
            propertyTypes
                    //Exclude any property from the join where no property is authorized.
                    .filter { pt -> authorizedPropertyTypes.any { it.value.contains(pt.key) } }
                    .map {
                        val joinType = if (propertyTypeFilters.containsKey(it.key)) INNER_JOIN else LEFT_JOIN
                        val propertyTypeEntitiesClause = buildPropertyTypeEntitiesClause(
                                entityKeyIds,
                                it.key,
                                authorizedPropertyTypes,
                                linking
                        )
                        val subQuerySql = selectCurrentVersionOfPropertyTypeSql(
                                propertyTypeEntitiesClause,
                                it.key,
                                propertyTypeFilters[it.key] ?: setOf<Filter>(),
                                it.value,
                                linking,
                                binaryPropertyTypes[it.key]!!,
                                metadataFilters
                        )
                        "$joinType $subQuerySql USING (${joinColumns.joinToString(",")})"
                    }
                    .joinToString("\n")

    val fullQuery = "SELECT DISTINCT $dataColumns FROM $entitiesSubquerySql $propertyTableJoins"
    return fullQuery
}


/**
 *
 */
fun selectEntitySetWithPropertyTypesAndVersionSql(
        entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
        propertyTypes: Map<UUID, String>,
        returnedPropertyTypes: Collection<UUID>,
        authorizedPropertyTypes: Map<UUID, Set<UUID>>,
        propertyTypeFilters: Map<UUID, Set<Filter>>,
        metadataOptions: Set<MetadataOption>,
        version: Long,
        linking: Boolean,
        binaryPropertyTypes: Map<UUID, Boolean>
): String {
    val entitiesClause = buildEntitiesClause(entityKeyIds, linking)
    val entitiesSubquerySql = selectEntityKeyIdsFilteredByVersionSubquerySql(entitiesClause, version, metadataOptions)

    val joinColumns =
            if (linking) {
                listOf(LINKING_ID.name)
            } else {
                listOf(ENTITY_SET_ID.name, ID_VALUE.name)
            }

    val dataColumns = joinColumns
            .union(metadataOptions.map(MetadataOption::name))
            .union(returnedPropertyTypes.map { propertyTypes[it]!! })
            .filter(String::isNotBlank)
            .joinToString(",")

    val propertyTableJoins =
            propertyTypes
                    //Exclude any property from the join where no property is authorized.
                    .filter { pt -> authorizedPropertyTypes.any { it.value.contains(pt.key) } }
                    .map {
                        val joinType = if (propertyTypeFilters.containsKey(it.key)) INNER_JOIN else LEFT_JOIN
                        val propertyTypeEntitiesClause = buildPropertyTypeEntitiesClause(
                                entityKeyIds,
                                it.key,
                                authorizedPropertyTypes,
                                linking
                        )
                        val subQuerySql = selectVersionOfPropertyTypeInEntitySetSql(
                                propertyTypeEntitiesClause,
                                it.key,
                                it.value,
                                version,
                                propertyTypeFilters[it.key] ?: setOf<Filter>(),
                                linking,
                                binaryPropertyTypes[it.key]!!
                        )
                        "$joinType $subQuerySql USING (${joinColumns.joinToString(",")}) "
                    }
                    .joinToString("\n")

    return "SELECT $dataColumns FROM $entitiesSubquerySql as $ENTITIES_TABLE_ALIAS $propertyTableJoins"

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
internal fun selectVersionOfPropertyTypeInEntitySetSql(
        entitiesClause: String,
        propertyTypeId: UUID,
        fqn: String,
        version: Long,
        filters: Set<Filter>,
        linking: Boolean,
        binary: Boolean
): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))

    val selectColumns =
            if (linking) LINKING_ID.name
            else entityKeyIdColumns

    val arrayAgg = arrayAggSql(fqn, binary)

    val selectPropertyTypeIdsFilteredByVersion = selectPropertyTypeDataKeysFilteredByVersionSubquerySql(
            entitiesClause,
            buildFilterClause(fqn, filters),
            propertyTable,
            version
    )

    /*
     * The reason that the linking id is joined in at this level is so that filtering works properly when tables are
     * joined together.
     */
    val linkingIdSubquerySql =
            if (linking) {
                "INNER JOIN (SELECT $entityKeyIdColumns,${LINKING_ID.name} FROM ${IDS.name}) as linking_ids USING($entityKeyIdColumns)"
            } else {
                ""
            }
    //We do an inner join here since we are effectively doing a self-join.
    return "(SELECT $selectColumns, $arrayAgg " +
            "FROM $selectPropertyTypeIdsFilteredByVersion " +
            linkingIdSubquerySql +
            "INNER JOIN $propertyTable USING($entityKeyIdColumns) " +
            "GROUP BY($selectColumns)) as $propertyTable "
}

internal fun selectCurrentVersionOfPropertyTypeSql(
        entitiesClause: String,
        propertyTypeId: UUID,
        filters: Set<Filter>,
        fqn: String,
        linking: Boolean,
        binary: Boolean,
        metadataFilters: String = ""
): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))

    val selectColumns =
            if (linking) LINKING_ID.name
            else entityKeyIdColumns

    val arrayAgg = arrayAggSql(fqn, binary)

    val filtersClause = buildFilterClause(fqn, filters)

    val linkingIdSubquerySql =
            if (linking) {
                "INNER JOIN (SELECT $entityKeyIdColumns,${LINKING_ID.name} FROM ${IDS.name}) as linking_ids USING($entityKeyIdColumns)"
            } else {
                ""
            }

    return "(SELECT $selectColumns, $arrayAgg " +
            "FROM $propertyTable " +
            linkingIdSubquerySql +
            "WHERE ${VERSION.name} > 0 $entitiesClause $filtersClause $metadataFilters" +
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
 *  If metadata options are reuquested it does an additional level of nesting to get metadata from entity key ids
 *  table.
 *
 * @param entitiesClause The clause that controls which entities will be evaluated by this query.
 * @param version The version of the properties being read.
 *
 * @return A named SQL subquery fragment consisting of all entity key satisfying the provided clauses from the
 * ids table.
 */
internal fun selectEntityKeyIdsFilteredByVersionSubquerySql(
        entitiesClause: String,
        version: Long,
        metadataOptions: Set<MetadataOption>
): String {
    return if (metadataOptions.isEmpty()) {
        "(SELECT $entityKeyIdColumns " +
                "FROM ( SELECT $entityKeyIdColumns, max(versions) as abs_max, max(abs(versions)) as max_abs " +
                "       FROM (  SELECT $entityKeyIdColumns, unnest(versions) as versions " +
                "           FROM ${IDS.name} " +
                "           WHERE $entitiesClause ) as $EXPANDED_VERSIONS " +
                "       WHERE abs(versions) <= $version " +
                "       GROUP BY($entityKeyIdColumns)) as unfiltered_data_keys " +
                "WHERE max_abs=abs_max ) as data_keys "
    } else {
        val metadataColumns = metadataOptions.map(ResultSetAdapters::mapMetadataOptionToPostgresColumn).joinToString(",") { it.name }
        return "(SELECT $entityKeyIdColumns,$metadataColumns FROM ${IDS.name} INNER JOIN (SELECT $entityKeyIdColumns " +
                "FROM ( SELECT $entityKeyIdColumns, max(versions) as abs_max, max(abs(versions)) as max_abs " +
                "       FROM (  SELECT $entityKeyIdColumns, unnest(versions) as versions " +
                "           FROM ${IDS.name} " +
                "           WHERE $entitiesClause ) as $EXPANDED_VERSIONS " +
                "       WHERE abs(versions) <= $version " +
                "       GROUP BY($entityKeyIdColumns)) as unfiltered_data_keys " +
                "WHERE max_abs=abs_max ) as data_keys ) as decorated_keys USING ($entityKeyIdColumns)"
    }

}

/**
 * This routine generates SQL that selects all entity key ids that are not tombstoned for the given entities clause. If
 * metadata is requested on a linked read, behavior is undefined as it will end up
 *
 * @param entitiesClause The clause that controls which entities will be evaluated by this query.
 * @param metadataOptions The metadata to return for the entity.
 *
 * @return A named SQL subquery fragment consisting of all entity key satisfying the provided clauses from the
 * ids table.
 */
internal fun selectEntityKeyIdsWithCurrentVersionSubquerySql(
        entitiesClause: String,
        metadataOptions: Set<MetadataOption>,
        linking: Boolean
): String {
    val metadataColumns = metadataOptions.map(ResultSetAdapters::mapMetadataOptionToPostgresColumn).joinToString(",") { it.name }
    val selectColumns = entityKeyIdColumns +
            (if(!metadataColumns.isEmpty()) ", $metadataColumns" else "" ) +
            if(linking) ", ${LINKING_ID.name}" else ""

    return "(SELECT $selectColumns FROM ${IDS.name} WHERE ${VERSION.name} > 0 $entitiesClause ) as $ENTITIES_TABLE_ALIAS"

}

internal fun arrayAggSql(fqn: String, binary: Boolean): String {
    return if (binary) " array_agg(encode($fqn, 'base64')) as $fqn "
    else " array_agg($fqn) as $fqn "
}

internal fun buildEntitiesClause( entityKeyIds: Map<UUID, Optional<Set<UUID>>>, linking: Boolean ): String {
    if (entityKeyIds.isEmpty()) return ""

    val filterLinkingIds = if(linking) " AND ${LINKING_ID.name} IS NOT NULL " else ""

    val idsColumn = if( linking ) LINKING_ID.name else ID_VALUE.name
    return filterLinkingIds +
            " AND (" + entityKeyIds.entries.joinToString(" OR ") {
        val idsClause = it.value.map {
            " AND $idsColumn IN ('" + it.joinToString(
                    "','"
            ) { it.toString() } + "')"
        }.orElse("")
        " (${ENTITY_SET_ID.name} = '${it.key}' $idsClause)"
    } + ")"
}

internal fun buildPropertyTypeEntitiesClause(
        entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
        propertyTypeId: UUID,
        authorizedPropertyTypes: Map<UUID, Set<UUID>>,
        linking: Boolean
): String {
    /*
     * Filter out any entity sets for which you aren't authorized to read this property.
     * There should always be at least one entity set as this isn't invoked for any properties
     * with not readable entity sets.
     */
    val authorizedEntityKeyIds = entityKeyIds.filter {
        authorizedPropertyTypes[it.key]?.contains(propertyTypeId) ?: false
    }

    return buildEntitiesClause( authorizedEntityKeyIds, linking )
}


internal fun buildFilterClause(fqn: String, filter: Set<Filter>): String {
    if (filter.isEmpty()) return ""
    return filter.joinToString(" AND ", prefix = " AND ") { it.asSql(fqn) }
}

internal fun selectEntityKeysOfLinkingIds( linkingIds:Set<UUID> ): String {
    val linkingEntitiesClause = buildLinkingEntitiesClause(linkingIds)
    return "SELECT $entityKeyIdColumns FROM ${selectEntityKeyIdsWithCurrentVersionSubquerySql(linkingEntitiesClause, setOf(), true)}"
}

internal fun buildLinkingEntitiesClause(linkingIds:Set<UUID>): String {
    return " AND ${LINKING_ID.name} IN ( ${linkingIds.joinToString(",") { "'$it'" }} )"
}
