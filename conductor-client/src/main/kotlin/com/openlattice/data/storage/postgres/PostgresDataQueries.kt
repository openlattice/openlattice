package com.openlattice.data.storage.postgres


import com.geekbeast.postgres.IndexType
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.PostgresColumnDefinition
import com.geekbeast.postgres.PostgresDatatype
import com.openlattice.IdConstants
import com.openlattice.analysis.SqlBindInfo
import com.openlattice.analysis.requests.Filter
import com.openlattice.data.FilteredDataPageDefinition
import com.openlattice.data.PropertyUpdateType
import com.openlattice.data.storage.BindDetails
import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.SqlBinder
import com.openlattice.edm.PostgresEdmTypeConverter
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.*
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresDataTables.Companion.dataTableValueColumns
import com.openlattice.postgres.PostgresDataTables.Companion.getColumnDefinition
import com.openlattice.postgres.PostgresDataTables.Companion.getSourceDataColumnName
import com.openlattice.postgres.PostgresTable.DATA
import com.openlattice.postgres.PostgresTable.IDS
import java.sql.PreparedStatement
import java.util.*

/**
 * This class is responsible for generating all the SQL for creating, reading, upated, and deleting entities.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
internal class PostgresDataQueries


const val VALUE = "value"
const val PROPERTIES = "properties"
private const val PAGED_IDS_CTE_NAME = "page_of_ids_to_select"

val dataTableColumnsSql = PostgresDataTables.dataTableColumns.joinToString(",") { it.name }

// @formatter:off
val detailedValueColumnsSql =
    "COALESCE( " + dataTableValueColumns.joinToString(",") {
        "jsonb_agg(" +
            "json_build_object(" +
                "'$VALUE',${it.name}, " +
                "'${ID_VALUE.name}', ${ORIGIN_ID.name}, " +
                "'${LAST_WRITE.name}', ${LAST_WRITE.name}" +
            ")" +
        ") FILTER ( WHERE ${it.name} IS NOT NULL) "
} + ") as $PROPERTIES"

val valuesColumnsSql = "COALESCE( " + dataTableValueColumns.joinToString(",") {
    "jsonb_agg(${it.name}) " +
    "FILTER (WHERE ${it.name} IS NOT NULL)"
} + ") as $PROPERTIES"
// @formatter:on

val primaryKeyColumnNamesAsString = PostgresDataTables.buildDataTableDefinition().primaryKey.joinToString(
        ","
) { it.name }


/**
 * Builds a preparable SQL query for reading filterable data.
 *
 * The first three columns om
 * @return A preparable sql query to read the data to a ordered set of [SqlBinder] objects. The prepared query
 * must be have the first three parameters bound separately from the [SqlBinder] objects. The parameters are as follows:
 * 1. entity set ids (array)
 * 2. entity key ids (array)
 *
 */
fun buildPreparableFiltersSql(
        propertyTypes: Map<UUID, PropertyType>,
        propertyTypeFilters: Map<UUID, Set<Filter>>,
        metadataOptions: Set<MetadataOption>,
        linking: Boolean,
        entitySetIds: Set<UUID>,
        entityKeyIds: Set<UUID>,
        detailed: Boolean = false,
        filteredDataPageDefinition: FilteredDataPageDefinition? = null
): Pair<String, Set<SqlBinder>> {
    var index = 1
    val binders = mutableSetOf<SqlBinder>()

    val (sqlClauses, filterBinders, nextIndex) = filteredDataPagePrefixAndSuffix(
            index,
            filteredDataPageDefinition,
            propertyTypes,
            entitySetIds,
            entityKeyIds
    )
    binders.addAll(filterBinders)
    index = nextIndex
    val (prefix, filterIdsOnCTEClause, suffix) = sqlClauses

    binders.add(SqlBinder(SqlBindInfo(index++, entitySetIds), ::doBind))
    if (entityKeyIds.isNotEmpty()) {
        binders.add(SqlBinder(SqlBindInfo(index++, entityKeyIds), ::doBind))
    }

    val (filterClauseSql, filterClauseBinders) = buildPreparableFiltersClause(index, propertyTypes, propertyTypeFilters)
    val filtersClause = if (filterClauseSql.isNotEmpty()) " AND $filterClauseSql " else ""
    binders.addAll(filterClauseBinders)

    val metadataOptionColumns = metadataOptions.associateWith(::mapOuterMetaDataToColumnSql)
    val metadataOptionColumnsSql = metadataOptionColumns.values.joinToString("")

    val innerGroupBy = if (linking) groupBy(ESID_ORIGINID_PART_PTID) else groupBy(ESID_EKID_PART_PTID)
    val outerGroupBy = if (metadataOptions.contains(MetadataOption.ENTITY_KEY_IDS)) groupBy(
            ESID_EKID_PART_PTID
    ) else groupBy(
            ESID_EKID_PART
    )

    val linkingClause = if (linking) " AND ${ORIGIN_ID.name} != '${IdConstants.EMPTY_ORIGIN_ID.id}' " else ""

    val innerSql = selectEntitiesGroupedByIdAndPropertyTypeId(
            metadataOptions,
            idsPresent = entityKeyIds.isNotEmpty(),
            detailed = detailed,
            linking = linking
    ) + linkingClause + filtersClause + filterIdsOnCTEClause + innerGroupBy

    val sql = """
        $prefix
        SELECT
          ${ENTITY_SET_ID.name},
          ${ID_VALUE.name}
          $metadataOptionColumnsSql,
          jsonb_object_agg(${PROPERTY_TYPE_ID.name}, $PROPERTIES) as $PROPERTIES
        FROM ($innerSql) entities
        $outerGroupBy
        $suffix
    """.trimIndent()

    return sql to binders
}

internal fun filteredDataPagePrefixAndSuffix(
        startIndex: Int,
        filteredDataPageDefinition: FilteredDataPageDefinition?,
        propertyTypes: Map<UUID, PropertyType>,
        entitySetIds: Set<UUID>,
        entityKeyIds: Set<UUID>
): Triple<List<String>, Set<SqlBinder>, Int> {
    var index = startIndex

    if (filteredDataPageDefinition == null) {
        return Triple(listOf("", "", ""), setOf(), index)
    }

    val selectFilters = mutableSetOf("${ENTITY_SET_ID.name} = ANY(?)")

    val sqlBinders = mutableSetOf(SqlBinder(SqlBindInfo(index++, entitySetIds), ::doBind))

    if (entityKeyIds.isNotEmpty()) {
        selectFilters.add("${ID.name} = ANY(?)")
        sqlBinders.add(SqlBinder(SqlBindInfo(index++, entityKeyIds), ::doBind))
    }

    if (filteredDataPageDefinition.bookmarkId != null) {
        selectFilters.add("${ID.name} > ?")
        sqlBinders.add(SqlBinder(SqlBindInfo(index++, filteredDataPageDefinition.bookmarkId!!), ::doBind))
    }

    val filtersMap = filteredDataPageDefinition.propertyTypeId?.let {
        mapOf(
                it to setOf(filteredDataPageDefinition.filter!!)
        )
    } ?: mapOf()
    val (filterSql, binders, nextIndex) = buildPreparableFiltersClause(index, propertyTypes, filtersMap)

    if (filterSql.isNotBlank()) {
        selectFilters.add(filterSql)
        sqlBinders.addAll(binders)
    }

    val prefix = """
        WITH $PAGED_IDS_CTE_NAME AS (
          SELECT DISTINCT ${ID.name}
          FROM ${DATA.name}
          WHERE ${selectFilters.joinToString(" AND ")}
          ORDER BY ${ID.name}
          LIMIT ${filteredDataPageDefinition.pageSize}
        )
    """.trimIndent()

    val filterClause = " AND ${ID.name} IN (SELECT ${ID.name} FROM $PAGED_IDS_CTE_NAME)"

    val suffix = " ORDER BY ${ID.name}"

    val sqlClauses = listOf(prefix, filterClause, suffix)
    return Triple(sqlClauses, sqlBinders, nextIndex)
}

internal fun selectEntitiesGroupedByIdAndPropertyTypeId(
        metadataOptions: Set<MetadataOption>,
        idsPresent: Boolean = true,
        entitySetsPresent: Boolean = true,
        detailed: Boolean = false,
        linking: Boolean = false
): String {
    //Already have the comma prefix
    val metadataOptionsSql = metadataOptions.joinToString("") { mapMetaDataToSelector(it) }
    val columnsSql = if (detailed) detailedValueColumnsSql else valuesColumnsSql
    val idColumn = if (linking) ORIGIN_ID.name else ID_VALUE.name
    return """
        SELECT
          ${ENTITY_SET_ID.name},
          $idColumn as ${ID_VALUE.name},
          ${PROPERTY_TYPE_ID.name}
          $metadataOptionsSql,
          $columnsSql
        FROM ${DATA.name}
        ${optionalWhereClauses(idsPresent, entitySetsPresent, linking)}
    """.trimIndent()
}

/**
 * Returns the correspondent column name used for the metadata option with a comma prefix.
 */
private fun mapOuterMetaDataToColumnSql(metadataOption: MetadataOption): String {
    return when (metadataOption) {
        // TODO should be just last_write with comma prefix after empty rows are eliminated https://jira.openlattice.com/browse/LATTICE-2254
        MetadataOption.LAST_WRITE -> ",max(${LAST_WRITE.name}) AS ${mapMetaDataToColumnName(metadataOption)}"
        MetadataOption.ENTITY_KEY_IDS -> ",array_agg(${ORIGIN_ID.name}) as ${ENTITY_KEY_IDS_COL.name}"
        else -> throw UnsupportedOperationException("No implementation yet for metadata option $metadataOption")
    }
}

/**
 * Returns the correspondent column name used for the metadata option.
 */
private fun mapMetaDataToColumnName(metadataOption: MetadataOption): String {
    return when (metadataOption) {
        MetadataOption.LAST_WRITE -> LAST_WRITE.name
        MetadataOption.ENTITY_KEY_IDS -> ENTITY_KEY_IDS_COL.name
        else -> throw UnsupportedOperationException("No implementation yet for metadata option $metadataOption")
    }
}

/**
 * Returns the select sql snippet for the requested metadata option.
 */
private fun mapMetaDataToSelector(metadataOption: MetadataOption): String {
    return when (metadataOption) {
        MetadataOption.LAST_WRITE -> ",max(${LAST_WRITE.name}) AS ${mapMetaDataToColumnName(metadataOption)}"
        MetadataOption.ENTITY_KEY_IDS -> ",${ID_VALUE.name} as ${ORIGIN_ID.name}" //Adapter for queries for now.
        else -> throw UnsupportedOperationException("No implementation yet for metadata option $metadataOption")
    }
}

/*
 * Creates a preparable query with the following clauses.
 */
private fun buildPreparableFiltersClause(
        startIndex: Int,
        propertyTypes: Map<UUID, PropertyType>,
        propertyTypeFilters: Map<UUID, Set<Filter>>
): Triple<String, Set<SqlBinder>, Int> {
    val bindList = propertyTypeFilters.entries
            .filter { (_, filters) -> filters.isNotEmpty() }
            .flatMap { (propertyTypeId, filters) ->
                val colName = getSourceDataColumnName(propertyTypes.getValue(propertyTypeId))

                //Generate sql preparable sql fragments
                var currentIndex = startIndex
                val filterFragments = filters.map { filter ->
                    val bindDetails = buildBindDetails(currentIndex, propertyTypeId, filter, colName)
                    currentIndex = bindDetails.nextIndex
                    bindDetails
                }

                filterFragments
            }

    val sql = bindList.joinToString(" AND ") { "(${it.sql})" }
    val bindInfo = bindList.flatMap { it.bindInfo }.toSet()
    val nextIndex = bindList.map { it.nextIndex }.maxOrNull() ?: startIndex
    return Triple(sql, bindInfo, nextIndex)
}

private fun buildBindDetails(
        startIndex: Int,
        propertyTypeId: UUID,
        filter: Filter,
        col: String
): BindDetails {
    val bindInfo = linkedSetOf<SqlBinder>()
    bindInfo.add(SqlBinder(SqlBindInfo(startIndex, propertyTypeId), ::doBind))
    bindInfo.addAll(filter.bindInfo(startIndex + 1).map { SqlBinder(it, ::doBind) })
    return BindDetails(startIndex + bindInfo.size, bindInfo, "${PROPERTY_TYPE_ID.name} = ? AND " + filter.asSql(col))
}

@Suppress("UNCHECKED_CAST")
internal fun doBind(ps: PreparedStatement, info: SqlBindInfo) {
    when (val v = info.value) {
        is String -> ps.setString(info.bindIndex, v)
        is Int -> ps.setInt(info.bindIndex, v)
        is Long -> ps.setLong(info.bindIndex, v)
        is Boolean -> ps.setBoolean(info.bindIndex, v)
        is Short -> ps.setShort(info.bindIndex, v)
        is java.sql.Array -> ps.setArray(info.bindIndex, v)
        //TODO: Fix this bustedness.
        is Collection<*> -> {
            val array = when (val elem = v.first()!!) {
                is String -> PostgresArrays.createTextArray(ps.connection, v as Collection<String>)
                is Int -> PostgresArrays.createIntArray(ps.connection, v as Collection<Int>)
                is Long -> PostgresArrays.createLongArray(ps.connection, v as Collection<Long>)
                is Boolean -> PostgresArrays.createBooleanArray(ps.connection, v as Collection<Boolean>)
                is Short -> PostgresArrays.createShortArray(ps.connection, v as Collection<Short>)
                is UUID -> PostgresArrays.createUuidArray(ps.connection, v as Collection<UUID>)
                else -> throw IllegalArgumentException(
                        "Collection with elements of ${elem.javaClass} are not " +
                                "supported in filters"
                )
            }
            ps.setArray(info.bindIndex, array)
        }
        else -> ps.setObject(info.bindIndex, v)
    }
}

internal val ESID_EKID_PART = "${ENTITY_SET_ID.name},${ID_VALUE.name}"
internal val ESID_EKID_PART_PTID = "$ESID_EKID_PART,${PROPERTY_TYPE_ID.name}"
internal val ESID_ORIGINID_PART_PTID = "${ENTITY_SET_ID.name},${ORIGIN_ID.name},${PROPERTY_TYPE_ID.name}"

internal fun groupBy(columns: String): String {
    return "GROUP BY ($columns)"
}

/**
 * Preparable SQL that selects entities across multiple entity sets grouping by id and property type id from the [DATA]
 * table with the following bind order:
 *
 * 1. entity set ids (array)
 * 2. entity key ids (array)
 *
 */

fun optionalWhereClauses(
        idsPresent: Boolean = true,
        entitySetsPresent: Boolean = true,
        linking: Boolean = false
): String {
    val entitySetClause = if (entitySetsPresent) "${ENTITY_SET_ID.name} = ANY(?)" else ""
    val idsColumn = if (linking) ORIGIN_ID.name else ID_VALUE.name
    val idsClause = if (idsPresent) "$idsColumn = ANY(?)" else ""
    val versionsClause = "${VERSION.name} > 0 "

    val optionalClauses = listOf(entitySetClause, idsClause, versionsClause).filter { it.isNotBlank() }
    return "WHERE ${optionalClauses.joinToString(" AND ")}"
}

fun optionalWhereClausesSingleEdk(
        idPresent: Boolean = true,
        entitySetPresent: Boolean = true
): String {
    val entitySetClause = if (entitySetPresent) "${ENTITY_SET_ID.name} = ?" else ""
    val idsClause = if (idPresent) "${ID_VALUE.name} = ?" else ""
    val versionsClause = "${VERSION.name} > 0 "

    val optionalClauses = listOf(entitySetClause, idsClause, versionsClause).filter { it.isNotBlank() }
    return "WHERE ${optionalClauses.joinToString(" AND ")}"
}

/**
 * Preparable sql to lock entities in [IDS] table.
 *
 * This query will lock provided entities that have an assigned linking id in ID order.
 *
 * The bind order is the following:
 *
 * 1 - entity set id
 *
 * 2 - entity key id
 *
 */
val lockEntitiesInIdsTable =
        "SELECT 1 FROM ${IDS.name} " +
                "WHERE ${ENTITY_SET_ID.name} = ? AND ${ID_VALUE.name} = ? " +
                "FOR UPDATE"

/**
 * Preparable sql to lock entities in [IDS] table.
 *
 * This query will lock provided entities that have an assigned linking id in ID order.
 *
 * The bind order is the following:
 *
 * 1 - entity key ids
 *
 */
val bulkLockEntitiesInIdsTable =
        "SELECT 1 FROM ${IDS.name} " +
                "WHERE ${ID_VALUE.name} = ANY(?) " +
                "ORDER BY ${ID_VALUE.name}" +
                "FOR UPDATE"


/**
 * Preparable sql to lock entities in [IDS] table.
 *
 * This query will lock provided entities that have an assigned linking id in ID order.
 *
 * The bind order is the following:
 *
 * 1 - entity set id
 *
 * 2 - entity key ids
 *
 */
val lockLinkedEntitiesInIdsTable =
        "SELECT 1 FROM ${IDS.name} " +
                "WHERE ${ENTITY_SET_ID.name} = ? AND ${ID_VALUE.name} = ANY(?) AND LINKING_ID IS NOT NULL " +
                "ORDER BY ${ID_VALUE.name} " +
                "FOR UPDATE"

/**
 * Preparable sql to upsert entities in [IDS] table.
 *
 * It sets a positive version and updates last write to current time.
 *
 * The bind order is the following:
 *
 * 1 - versions
 *
 * 2 - version
 *
 * 3 - version
 *
 * 4 - entity set id
 *
 * 5 - entity key ids
 *
 */
// @formatter:off
val upsertEntitiesSql = "UPDATE ${IDS.name} " +
        "SET ${VERSIONS.name} = ${VERSIONS.name} || ?, " +
            "${LAST_WRITE.name} = now(), " +
            "${VERSION.name} = CASE " +
                "WHEN abs(${IDS.name}.${VERSION.name}) <= abs(?) THEN ? " +
                "ELSE ${IDS.name}.${VERSION.name} " +
            "END " +
        "WHERE ${ENTITY_SET_ID.name} = ? AND ${ID_VALUE.name} = ANY(?) "
// @formatter:on

/**
 * Preparable sql to update an entity in the [IDS] table.
 *
 * It sets a positive version and updates last write to current time.
 *
 * The bind order is the following:
 *
 * 1 - versions
 *
 * 2 - version
 *
 * 3 - version
 *
 * 4 - entity set id
 *
 * 5 - entity key id
 *
 */
// @formatter:off
val updateEntitySql = "UPDATE ${IDS.name} " +
        "SET ${VERSIONS.name} = ${VERSIONS.name} || ?, " +
        "${LAST_WRITE.name} = now(), " +
        "${VERSION.name} = CASE " +
        "WHEN abs(${IDS.name}.${VERSION.name}) <= abs(?) THEN ? " +
        "ELSE ${IDS.name}.${VERSION.name} " +
        "END " +
        "WHERE ${ENTITY_SET_ID.name} = ? AND ${ID_VALUE.name} = ? "
// @formatter:on

/**
 * Preparable SQL that upserts a version and sets last write to current datetime for all entities in a given entity set
 * in [IDS] table.
 *
 * The following bind order is expected:
 *
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 */
// @formatter:off
internal val updateVersionsForEntitySet = "UPDATE ${IDS.name} " +
        "SET " +
            "${VERSIONS.name} = ${VERSIONS.name} || ARRAY[?], " +
            "${VERSION.name} = " +
                "CASE " +
                    "WHEN abs(${IDS.name}.${VERSION.name}) <= abs(?) " +
                    "THEN ? " +
                    "ELSE ${IDS.name}.${VERSION.name} " +
                "END, " +
            "${LAST_WRITE.name} = 'now()' " +
        "WHERE ${ENTITY_SET_ID.name} = ? "
// @formatter:on
/**
 * Preparable SQL that upserts a version and sets last write to current datetime for all entities in a given entity set
 * in [IDS] table.
 *
 * The following bind order is expected:
 *
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 * 5. entity key ids (uuid array)
 */
internal val updateVersionsForEntitiesInEntitySet = "$updateVersionsForEntitySet " +
        "AND ${ID_VALUE.name} = ANY(?) "

/**
 * Preparable SQL that zeroes out the version and sets last write to current datetime for all entities in a given
 * entity set in [IDS] table.
 *
 * The following bind order is expected:
 *
 * 1. entity set id
 */
// @formatter:off
internal val zeroVersionsForEntitySet = "UPDATE ${IDS.name} " +
        "SET " +
            "${VERSIONS.name} = ${VERSIONS.name} || ARRAY[0]::bigint[], " +
            "${VERSION.name} = 0, " +
            "${LAST_WRITE.name} = 'now()' " +
        "WHERE " +
            "${ENTITY_SET_ID.name} = ? "
// @formatter:on


/**
 * Preparable SQL that zeroes out the version and sets last write to current datetime for all entities in a given
 * entity set in [IDS] table.
 *
 * The following bind order is expected:
 *
 * 1. entity set id
 * 2. id (uuid array)
 */
internal val zeroVersionsForEntitiesInEntitySet = "$zeroVersionsForEntitySet AND ${ID.name} = ANY(?) "

/**
 * Preparable SQL that updates a version and sets last write to current datetime for all properties in a given entity
 * set in [DATA] table.
 *
 * The following bind order is expected:
 *
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 */
// @formatter:off
internal val updateVersionsForPropertiesInEntitySet = "UPDATE ${DATA.name} " +
        "SET " +
            "${VERSIONS.name} = ${VERSIONS.name} || ARRAY[?], " +
            "${VERSION.name} = " +
                "CASE " +
                    "WHEN abs(${DATA.name}.${VERSION.name}) <= abs(?) " +
                    "THEN ? " +
                    "ELSE ${DATA.name}.${VERSION.name} " +
                "END, " +
            "${LAST_WRITE.name} = 'now()' " +
        "WHERE ${ENTITY_SET_ID.name} = ? "

// @formatter:on
/**
 * Preparable SQL that updates a version and sets last write to current datetime for all properties in a given entity
 * set in [DATA] table.
 *
 * The following bind order is expected:
 *
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 * 5. property type ids
 */
internal val updateVersionsForPropertyTypesInEntitySet = "$updateVersionsForPropertiesInEntitySet " +
        "AND ${PROPERTY_TYPE_ID.name} = ANY(?)"

/**
 * Preparable SQL that updates a version and sets last write to current datetime for all properties in a given entity
 * set in [DATA] table.
 *
 * The following bind order is expected:
 *
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 * 5. property type ids
 * 6. entity key ids
 *    IF LINKING    checks against ORIGIN_ID
 *    ELSE          checks against ID column
 */
fun updateVersionsForPropertyTypesInEntitiesInEntitySet(linking: Boolean = false): String {
    val idsSql = if (linking) {
        "AND ${ORIGIN_ID.name} = ANY(?)"
    } else {
        "AND ${ID_VALUE.name} = ANY(?)"
    }

    return "$updateVersionsForPropertyTypesInEntitySet $idsSql "
}

/**
 * Preparable SQL updates a version and sets last write to current datetime for all property values in a given entity
 * set in [DATA] table.
 *
 * The following bind order is expected:
 *
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 * 5. property type ids
 * 6. entity key ids (if linking: linking ids)
 *    {origin ids}: only if linking
 * 7. hash
 */
internal fun updateVersionsForPropertyValuesInEntitiesInEntitySet(linking: Boolean = false): String {
    return "${updateVersionsForPropertyTypesInEntitiesInEntitySet(linking)} AND ${HASH.name} = ? "
}

/**
 * Preparable SQL that updates a version and sets last write to current datetime for all properties in a given linked
 * entity set in [DATA] table.
 *
 * Update set:
 * 1. VERSION: system.currentTime
 * 2. VERSION: system.currentTime
 * 3. VERSION: system.currentTime
 *
 * Where :
 * 4. ENTITY_SET: entity set id
 * 5. ID_VALUE: linking id
 * 6. ORIGIN_ID: entity key id
 */
val tombstoneLinkForEntity = "$updateVersionsForPropertiesInEntitySet " +
        "AND ${ID_VALUE.name} = ? " +
        "AND ${ORIGIN_ID.name} = ? "

/**
 * Preparable SQL deletes a given property in a given entity set in [DATA]
 *
 * The following bind order is expected:
 *
 * 1. entity set id
 * 2. property type id
 */
internal val deletePropertyInEntitySet = "DELETE FROM ${DATA.name} WHERE ${ENTITY_SET_ID.name} = ? AND ${PROPERTY_TYPE_ID.name} = ? "

/**
 * Preparable SQL deletes all entity ids a given entity set in [IDS]
 *
 * The following bind order is expected:
 *
 * 1. entity set id
 */
internal val deleteEntitySetEntityKeys = "DELETE FROM ${IDS.name} WHERE ${ENTITY_SET_ID.name} = ? "

/**
 * Preparable SQL that deletes selected property values of entities and their linking entities in a given entity set
 * in [DATA] table.
 *
 * The following bind order is expected:
 *
 * 1. entity set id
 * 2. entity key ids (non-linking entities, ID column)
 * 3. entity key ids (linking entities, ORIGIN_ID column)
 * 4. property type ids
 */
// @formatter:off
internal val deletePropertiesOfEntitiesInEntitySet =
        "DELETE FROM ${DATA.name} " +
        "WHERE ${ENTITY_SET_ID.name} = ? AND " +
              "( ${ID_VALUE.name} = ANY( ? ) OR ${ORIGIN_ID.name} = ANY( ? ) ) AND " +
              "${PROPERTY_TYPE_ID.name} = ANY( ? ) "
    // @formatter:on

/**
 * Preparable SQL that deletes all property values and entity key id of entities and their linking entities in a given
 * entity set in [DATA] table.
 *
 * The following bind order is expected:
 *
 * 1. entity set id
 * 2. entity key ids (non-linking entities, ID column)
 * 3. entity key ids (linking entities, ORIGIN_ID column)
 */
internal val deleteEntitiesInEntitySet =
        "DELETE FROM ${DATA.name} " +
                "WHERE ${ENTITY_SET_ID.name} = ? AND " +
                "( ${ID_VALUE.name} = ANY( ? ) OR ${ORIGIN_ID.name} = ANY( ? ) ) "

/**
 * Preparable SQL deletes all entities in a given entity set in [IDS]
 *
 * The following bind order is expected:
 *
 * 1. entity set id
 * 2. entity key ids
 */
// @formatter:off
internal val deleteEntityKeys =
        "$deleteEntitySetEntityKeys AND " +
            "${ID.name} = ANY(?) "
// @formatter:on

/**
 * Selects a text properties from entity sets with the following bind order:
 * 1. entity set ids  (array)
 * 2. property type ids (array)
 *
 */
internal val selectEntitySetTextProperties = "SELECT COALESCE(${
    getSourceDataColumnName(
        PostgresDatatype.TEXT, IndexType.NONE
    )
},${getSourceDataColumnName(PostgresDatatype.TEXT, IndexType.BTREE)}) AS ${
    getMergedDataColumnName(
        PostgresDatatype.TEXT
    )
} " +
        "FROM ${DATA.name} " +
        "WHERE (${
            getSourceDataColumnName(
                PostgresDatatype.TEXT, IndexType.NONE
            )
        } IS NOT NULL OR ${getSourceDataColumnName(PostgresDatatype.TEXT, IndexType.BTREE)} IS NOT NULL) AND " +
        "${ENTITY_SET_ID.name} = ANY(?) AND ${PROPERTY_TYPE_ID.name} = ANY(?) "


/**
 * Selects a text properties from specific entities with the following bind order:
 * 1. entity set ids  (array)
 * 2. property type ids (array)
 * 3. entity key ids (array)
 */
internal val selectEntitiesTextProperties = "$selectEntitySetTextProperties AND ${ID_VALUE.name} = ANY(?)"

fun getMergedDataColumnName(datatype: PostgresDatatype): String {
    return "v_${datatype.name}"
}

/**
 * This function generates preparable sql with the following bind order:
 *
 * 1.  ENTITY_SET_ID
 * 2.  ID_VALUE
 * 3.  PROPERTY_TYPE_ID
 * 4.  HASH
 *     LAST_WRITE = now()
 * 5.  VERSIONS
 * 6.  Value Column
 */
fun upsertPropertyValueSql(propertyType: PropertyType, updateType: PropertyUpdateType): String {
    val insertColumn = getColumnDefinition(propertyType.postgresIndexType, propertyType.datatype)
    val metadataColumnsSql = listOf(
            ENTITY_SET_ID,
            ID_VALUE,
            PROPERTY_TYPE_ID,
            HASH,
            LAST_WRITE,
            VERSION,
            VERSIONS
    ).joinToString(",") { it.name }

    val whereClause = when (updateType) {
        PropertyUpdateType.Versioned -> ""
        PropertyUpdateType.Unversioned -> """
            WHERE ${DATA.name}.${VERSION.name} < 0 
        """.trimIndent()
    }
    return """
        INSERT INTO ${DATA.name} ($metadataColumnsSql,${insertColumn.name})
            VALUES (?,?,?,?,now(),?,?,?)
            ON CONFLICT ($primaryKeyColumnNamesAsString)
            DO UPDATE SET
                ${VERSIONS.name} = ${DATA.name}.${VERSIONS.name} || EXCLUDED.${VERSIONS.name},
                ${LAST_WRITE.name} = GREATEST(${DATA.name}.${LAST_WRITE.name},EXCLUDED.${LAST_WRITE.name}),
                ${ORIGIN_ID.name} = EXCLUDED.${ORIGIN_ID.name}, 
                ${VERSION.name} = CASE WHEN abs(${DATA.name}.${VERSION.name}) <= EXCLUDED.${VERSION.name}
                    THEN EXCLUDED.${VERSION.name}
                    ELSE ${DATA.name}.${VERSION.name} 
                END
            $whereClause
        """.trimIndent()
}

/**
 *
 * UPDATE DATA:
 * 1. Set ORIGIN_ID: linkingID
 * 2. where ENTITY_SET_ID: uuid
 * 3. and ID_VALUE: UUID
 */
fun updateLinkingId(): String {
    return """
        UPDATE ${DATA.name} SET ${ORIGIN_ID.name} = ?
        WHERE ${ENTITY_SET_ID.name} = ? AND ${ID_VALUE.name} = ? 
    """.trimIndent()
}

/**
 * Used to C(~RUD~) a link from linker
 * This function generates preparable sql with the following bind order:
 *
 * Insert into:
 * 1. ID_VALUE: linkingId
 * 2. VERSION: system.currentTime
 *
 * Select ƒrom where:
 * 3. ENTITY_SET: entity set id
 * 4. ID_VALUE: entity key id
 *
 */
fun createOrUpdateLinkFromEntity(): String {
    val existingColumnsUpdatedForLinking = PostgresDataTables.dataTableColumns.joinToString(",") {
        when (it) {
            VERSION, ID_VALUE -> "?"
            ORIGIN_ID -> ID_VALUE.name
            LAST_WRITE -> "now()"
            else -> it.name
        }
    }

    // @formatter:off
    return "INSERT INTO ${DATA.name} ($dataTableColumnsSql) " +
            "SELECT $existingColumnsUpdatedForLinking " +
            "FROM ${DATA.name} " +
            "${optionalWhereClausesSingleEdk(idPresent = true, entitySetPresent = true)} " +
            "ON CONFLICT ($primaryKeyColumnNamesAsString) " +
            "DO UPDATE SET " +
                "${VERSIONS.name} = ${DATA.name}.${VERSIONS.name} || EXCLUDED.${VERSIONS.name}, " +
                "${LAST_WRITE.name} = GREATEST(${DATA.name}.${LAST_WRITE.name},EXCLUDED.${LAST_WRITE.name}), " +
                "${ORIGIN_ID.name} = EXCLUDED.${ORIGIN_ID.name}, " +
                "${VERSION.name} = CASE " +
                    "WHEN abs(${DATA.name}.${VERSION.name}) <= EXCLUDED.${VERSION.name} " +
                    "THEN EXCLUDED.${VERSION.name} " +
                    "ELSE ${DATA.name}.${VERSION.name} " +
                "END"
    // @formatter:on
}

/**
 * Partitioning selector requires an unambiguous data column called partitions to exist in the query to correcly compute partitions.
 */
fun getPartitioningSelector(
        idColumn: PostgresColumnDefinition
) = getPartitioningSelector(idColumn.name)

/**
 * Partitioning selector requires an unambiguous data column called partitions to exist in the query to correcly compute partitions.
 */
fun getPartitioningSelector(
        idColumn: String
) = "partitions[ 1 + ((array_length(partitions,1) + (('x'||right(${idColumn}::text,8))::bit(32)::int % array_length(partitions,1))) % array_length(partitions,1))]"

/**
 * SQL that given an array of partitions, their length, and a uuid column [idColumn] selects a partition from the
 * array of partitions using the lower order 32 bits of the uuid.
 *
 * It does this by converting the uuid to text, taking the right-most 8 characters, prepending an x so that it will be interpreted as hex,
 * casts it to 32 bit string, cast those 32 bits as an int, then uses the size of partition to compute the one-based index
 * in the array. We do the mod twice to make sure that it is the positive remainder.
 *
 * 1. partitions (array)
 * 2. array length ( partitions )
 * 3. array length ( partitions )
 * 4. array length ( partitions )
 *
 * @param idColumn
 */
fun getDirectPartitioningSelector(
        idColumn: String
) = "(?)[ 1 + ((? + (('x'||right(${idColumn}::text,8))::bit(32)::int % ?)) % ?)]"

private fun selectPropertyColumn(propertyType: PropertyType): String {
    val dataType = PostgresEdmTypeConverter.map(propertyType.datatype).sql()
    val propertyColumnName = propertyColumnName(propertyType)

    return "jsonb_array_elements_text($PROPERTIES -> '${propertyType.id}')::$dataType AS $propertyColumnName"
}

private fun selectPropertyArray(propertyType: PropertyType): String {
    val propertyColumnName = propertyColumnName(propertyType)
    return if (propertyType.isMultiValued) {
        "array_agg($propertyColumnName) FILTER (WHERE $propertyColumnName IS NOT NULL) as $propertyColumnName"
    } else {
        "(array_agg($propertyColumnName))[1] FILTER (WHERE $propertyColumnName IS NOT NULL) as $propertyColumnName"
    }
}

private fun propertyColumnName(propertyType: PropertyType): String {
    return DataTables.quote(propertyType.type.fullQualifiedNameAsString)
}
