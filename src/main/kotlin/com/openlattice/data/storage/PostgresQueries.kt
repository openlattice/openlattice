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
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import org.slf4j.LoggerFactory
import java.util.*

const val FETCH_SIZE = 100000

/**
 * Handles reading data from linked entity sets.
 */
private val logger = LoggerFactory.getLogger(PostgresQueries::class.java)
const val ENTITIES_TABLE_ALIAS = "entity_key_ids"
const val LEFT_JOIN = "LEFT JOIN"
const val INNER_JOIN = "INNER JOIN"
const val FILTERED_ENTITY_KEY_IDS = "filtered_entity_key_ids"
val entityKeyIdColumns = listOf(ENTITY_SET_ID.name, ID_VALUE.name).joinToString(",")

private val ALLOWED_LINKING_METADATA_OPTIONS = EnumSet.of(
        MetadataOption.LAST_WRITE,
        MetadataOption.ENTITY_KEY_IDS,
        MetadataOption.ENTITY_SET_IDS
)

private val ALLOWED_NON_LINKING_METADATA_OPTIONS = EnumSet.allOf(MetadataOption::class.java).minus(
        listOf(
                MetadataOption.ENTITY_SET_IDS
        )
)

@Deprecated("old class")
internal class PostgresQueries

/**
 *
 */
@Deprecated("old query")
fun selectEntitySetWithCurrentVersionOfPropertyTypes(
        entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
        propertyTypes: Map<UUID, String>,
        returnedPropertyTypes: Collection<UUID>,
        authorizedPropertyTypes: Map<UUID, Set<UUID>>,
        propertyTypeFilters: Map<UUID, Set<Filter>>,
        metadataOptions: Set<MetadataOption>,
        binaryPropertyTypes: Map<UUID, Boolean>,
        linking: Boolean,
        omitEntitySetId: Boolean,
        metadataFilters: String = ""
): String {
    val entitiesClause = buildEntitiesClause(entityKeyIds, linking)
    val withClause = buildWithClauseOld(linking, entitiesClause)
    val joinColumns = getJoinColumns(linking, omitEntitySetId)

    val entitiesSubquerySql = selectEntityKeyIdsWithCurrentVersionSubquerySql(
            entitiesClause,
            metadataOptions,
            linking,
            omitEntitySetId,
            joinColumns
    )

    val dataColumns = joinColumns
            .union(getMetadataOptions(metadataOptions, linking))
            .union(returnedPropertyTypes.map { propertyTypes.getValue(it) })
            .joinToString(",")

    // for performance, we place inner joins of filtered property tables at the end
    val orderedPropertyTypesByJoin = propertyTypes.toList()
            .sortedBy { propertyTypeFilters.containsKey(it.first) }.toMap()
    val propertyTableJoins =
            orderedPropertyTypesByJoin
                    //Exclude any property from the join where no property is authorized.
                    .filter { pt -> authorizedPropertyTypes.any { it.value.contains(pt.key) } }
                    .map {
                        val joinType = if (propertyTypeFilters.containsKey(it.key)) INNER_JOIN else LEFT_JOIN
                        val propertyTypeEntitiesClause = buildPropertyTypeEntitiesClause(
                                entityKeyIds,
                                it.key,
                                authorizedPropertyTypes
                        )
                        val subQuerySql = selectCurrentVersionOfPropertyTypeSql(
                                propertyTypeEntitiesClause,
                                it.key,
                                propertyTypeFilters[it.key] ?: setOf(),
                                it.value,
                                joinColumns,
                                metadataFilters
                        )
                        "$joinType $subQuerySql USING (${joinColumns.joinToString(",")})"
                    }
                    .joinToString("\n")

    val fullQuery = "$withClause SELECT $dataColumns FROM $entitiesSubquerySql $propertyTableJoins"
    return fullQuery
}

@Deprecated("old query")
private fun buildWithClauseOld(linking: Boolean, entitiesClause: String): String {
    val joinColumns = if (linking) {
        listOf(ENTITY_SET_ID.name, ID_VALUE.name, LINKING_ID.name)
    } else {
        listOf(ENTITY_SET_ID.name, ID_VALUE.name)
    }
    val selectColumns = joinColumns.joinToString(",") { "${PostgresTable.IDS.name}.$it AS $it" }

    val queriesSql = "SELECT $selectColumns FROM ${PostgresTable.IDS.name} WHERE ${VERSION.name} > 0 $entitiesClause"

    return "WITH $FILTERED_ENTITY_KEY_IDS AS ( $queriesSql ) "
}


@Deprecated("old query")
internal fun selectCurrentVersionOfPropertyTypeSql(
        entitiesClause: String,
        propertyTypeId: UUID,
        filters: Set<Filter>,
        fqn: String,
        joinColumns: List<String>,
        metadataFilters: String = ""
): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))
    val groupByColumns = joinColumns.joinToString(",")
    val arrayAgg = " array_agg($fqn) as $fqn "
    val filtersClause = buildFilterClause(fqn, filters)

    return "(SELECT $groupByColumns, $arrayAgg " +
            "FROM $propertyTable INNER JOIN $FILTERED_ENTITY_KEY_IDS USING (${ENTITY_SET_ID.name},${ID_VALUE.name}) " +
            "WHERE ${VERSION.name} > 0 $entitiesClause $filtersClause $metadataFilters" +
            "GROUP BY ($groupByColumns)) as $propertyTable "
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
@Deprecated("old query")
internal fun selectEntityKeyIdsWithCurrentVersionSubquerySql(
        entitiesClause: String,
        metadataOptions: Set<MetadataOption>,
        linking: Boolean,
        omitEntitySetId: Boolean,
        joinColumns: List<String>
): String {
    val metadataColumns = getMetadataOptions(metadataOptions.minus(MetadataOption.ENTITY_KEY_IDS), linking)
            .joinToString(",")
    val joinColumnsSql = joinColumns.joinToString(",")
    val selectColumns = joinColumnsSql +
            // used in materialized entitysets for both linking and non-linking entity sets to join on edges
            if (metadataOptions.contains(MetadataOption.ENTITY_KEY_IDS)) {
                ", array_agg(${PostgresTable.IDS.name}.${ID.name}) AS " +
                        ResultSetAdapters.mapMetadataOptionToPostgresColumn(MetadataOption.ENTITY_KEY_IDS)
            } else {
                ""
            } +
            if (metadataColumns.isNotEmpty()) {
                if (linking) {
                    if (metadataOptions.contains(MetadataOption.LAST_WRITE)) {
                        ", max(${LAST_WRITE.name}) AS " +
                                ResultSetAdapters.mapMetadataOptionToPostgresColumn(MetadataOption.LAST_WRITE)
                    } else {
                        ""
                    } + if (metadataOptions.contains(MetadataOption.ENTITY_SET_IDS)) {
                        ", array_agg(${PostgresTable.IDS.name}.${ENTITY_SET_ID.name}) AS " +
                                ResultSetAdapters.mapMetadataOptionToPostgresColumn(MetadataOption.ENTITY_SET_IDS)
                    } else {
                        ""
                    }
                } else {
                    ", $metadataColumns"
                }
            } else {
                ""
            }

    val groupBy = if (linking) {
        if (omitEntitySetId) {
            "GROUP BY ${LINKING_ID.name}"
        } else {
            "GROUP BY (${ENTITY_SET_ID.name},${LINKING_ID.name})"
        }
    } else {
        if (metadataOptions.contains(MetadataOption.ENTITY_KEY_IDS)) {
            "GROUP BY (${ENTITY_SET_ID.name}, ${ID.name})"
        } else {
            ""
        }
    }

    return "( SELECT $selectColumns FROM ${PostgresTable.IDS.name} INNER JOIN $FILTERED_ENTITY_KEY_IDS USING($joinColumnsSql) WHERE true $entitiesClause $groupBy ) as $ENTITIES_TABLE_ALIAS"
}


@Deprecated("old query")
internal fun buildEntitiesClause(entityKeyIds: Map<UUID, Optional<Set<UUID>>>, linking: Boolean): String {
    if (entityKeyIds.isEmpty()) return ""

    val filterLinkingIds = if (linking) " AND ${LINKING_ID.name} IS NOT NULL " else ""

    val idsColumn = if (linking) LINKING_ID.name else ID_VALUE.name
    return "$filterLinkingIds AND (" + entityKeyIds.entries.joinToString(" OR ") {
        val idsClause = it.value
                .filter { ids -> ids.isNotEmpty() }
                .map { ids -> " $idsColumn IN (" + ids.joinToString(",") { id -> "'$id'" } + ") AND" }
                .orElse("")
        " ($idsClause ${PostgresTable.IDS.name}.${ENTITY_SET_ID.name} = '${it.key}' )"
    } + ")"
}

@Deprecated("old query")
internal fun buildPropertyTypeEntitiesClause(
        entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
        propertyTypeId: UUID,
        authorizedPropertyTypes: Map<UUID, Set<UUID>>
): String {
    /*
     * Filter out any entity sets for which you aren't authorized to read this property.
     * There should always be at least one entity set as this isn't invoked for any properties
     * with not readable entity sets.
     */
    val authorizedEntitySetIds = entityKeyIds.asSequence().filter {
        authorizedPropertyTypes[it.key]?.contains(propertyTypeId) ?: false
    }.joinToString(",") { "'${it.key}'" }

    return if (authorizedEntitySetIds.isNotEmpty()) "AND ${ENTITY_SET_ID.name} IN ($authorizedEntitySetIds)" else ""
}

@Deprecated("old query")
internal fun buildFilterClause(fqn: String, filter: Set<Filter>): String {
    if (filter.isEmpty()) return ""
    return filter.joinToString(" AND ", prefix = " AND ") { it.asSql(fqn) }
}


@Deprecated("old query")
private fun getJoinColumns(linking: Boolean, omitEntitySetId: Boolean): List<String> {
    return if (linking) {
        if (omitEntitySetId) {
            listOf(LINKING_ID.name)
        } else {
            listOf(ENTITY_SET_ID.name, LINKING_ID.name)
        }
    } else {
        listOf(ENTITY_SET_ID.name, ID_VALUE.name)
    }
}

@Deprecated("old query")
private fun getMetadataOptions(metadataOptions: Set<MetadataOption>, linking: Boolean): List<String> {
    val allowedMetadataOptions = if (linking)
        metadataOptions.intersect(ALLOWED_LINKING_METADATA_OPTIONS)
    else
        metadataOptions.intersect(ALLOWED_NON_LINKING_METADATA_OPTIONS)


    //TODO: Make this an error and fix cases by providing static method for getting all allowed metadata options
    //as opposed to just EnumSet.allOf(...)
    val invalidMetadataOptions = metadataOptions - allowedMetadataOptions
    if (invalidMetadataOptions.isNotEmpty()) {
        logger.warn("Invalid metadata options requested: {}", invalidMetadataOptions)
    }

    return allowedMetadataOptions.map { ResultSetAdapters.mapMetadataOptionToPostgresColumn(it) }
}