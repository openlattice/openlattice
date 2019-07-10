package com.openlattice.data.storage


import com.openlattice.analysis.SqlBindInfo
import com.openlattice.analysis.requests.Filter
import com.openlattice.edm.PostgresEdmTypeConverter
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.*
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresDataTables.Companion.getColumnDefinition
import com.openlattice.postgres.PostgresTable.DATA
import com.openlattice.postgres.PostgresTable.IDS
import java.lang.IllegalStateException
import java.sql.PreparedStatement
import java.util.*

/**
 * This class is responsible for generating all the SQL for creating, reading, upated, and deleting entities.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
internal class PostgresDataQueries

const val VALUES = "values"
val dataMetadataColumnsParametersSql = PostgresDataTables.dataTableMetadataColumns.joinToString(",") { "?" }
val dataMetadataColumnsSql = PostgresDataTables.dataTableMetadataColumns.joinToString { "," }
val dataTableColumnsSql = PostgresDataTables.dataTableColumns.joinToString(",") { it.name }
val dataTableColumnsBindSql = PostgresDataTables.dataTableColumns.joinToString(",") { "?" }

val dataTableColumnsConflictSetSql = PostgresDataTables.dataTableColumns.joinToString(",") {
    "${it.name} = EXCLUDED.${it.name}"
}

val valuesColumnsSql = PostgresDataTables.dataTableValueColumns.joinToString(",") {
    "array_agg(${it.name}) FILTER (where ${it.name} IS NOT NULL) as ${it.name}"
}

val jsonValueColumnsSql = PostgresDataTables.dataColumns.entries
        .joinToString(",") { (datatype, cols) ->
            val (ni, bt) = cols
            "COALESCE(jsonb_object_agg(${PROPERTY_TYPE_ID.name}, ${bt.name} || ${ni.name}) " +
                    "FILTER (WHERE ${bt.name} IS NOT NULL OR ${ni.name} IS NOT NULL ),'{}') " +
                    "as ${getMergedDataColumnName(datatype)}"
        }


/**
 * Builds a preparable SQL query for reading filterable data.
 *
 * The first three columns om
 * @return A preparable sql query to read the data to a ordered set of [SqlBinder] objects. The prepared query
 * must be have the first three parameters bound separately from the [SqlBinder] objects. The parameters are as follows:
 * 1. entity set ids (array)
 * 2. entity key ids (array)
 * 3. partition(s) (array)
 *
 */
fun buildPreparableFiltersClauseForLinkedEntities(
        startIndex: Int,
        propertyTypes: Map<UUID, PropertyType>,
        propertyTypeFilters: Map<UUID, Set<Filter>>,
        idsPresent: Boolean,
        partitionsPresent: Boolean
): Pair<String, Set<SqlBinder>> {
    val filtersClauses = buildPreparableFiltersClause(startIndex, propertyTypes, propertyTypeFilters)
    val filtersClause = filtersClauses.first
    val sql = selectEntitiesGroupedByIdAndPropertyTypeId(idsPresent = idsPresent, partitionsPresent = partitionsPresent)
    " AND ${ORIGIN_ID.name} IS NOT NULL AND ${VERSION.name} > 0" +
            " ${if (filtersClause.isNotEmpty()) "AND $filtersClause " else ""}" +
            "GROUP BY (${ENTITY_SET_ID.name},${LINKING_ID.name}, ${PARTITION.name}, ${PROPERTY_TYPE_ID.name})"

    return sql to filtersClauses.second

}

/**
 * Builds a preparable SQL query for reading filterable data.
 *
 * The first three columns om
 * @return A preparable sql query to read the data to a ordered set of [SqlBinder] objects. The prepared query
 * must be have the first three parameters bound separately from the [SqlBinder] objects. The parameters are as follows:
 * 1. entity set ids (array)
 * 2. entity key ids (array)
 * 3. partition(s) (array)
 *
 */
fun buildPreparableFiltersSqlForEntities(
        startIndex: Int,
        propertyTypes: Map<UUID, PropertyType>,
        propertyTypeFilters: Map<UUID, Set<Filter>>,
        idsPresent: Boolean,
        partitionsPresent: Boolean
): Pair<String, Set<SqlBinder>> {
    val filtersClauses = buildPreparableFiltersClause(startIndex, propertyTypes, propertyTypeFilters)
    //TODO: I'm pretty sure if propertyTypeFilters are entity this won't work properly.
    val filtersClause = filtersClauses.first
    val innerSql = selectEntitiesGroupedByIdAndPropertyTypeId(
            idsPresent = idsPresent, partitionsPresent = partitionsPresent
    ) +
            " ${if (filtersClause.isNotEmpty()) "AND $filtersClause " else ""}" +
            GROUP_BY_ESID_EKID_PART_PTID
    val sql = "SELECT ${ENTITY_SET_ID.name},${ID_VALUE.name},$jsonValueColumnsSql FROM ($innerSql) entities " +
            GROUP_BY_ESID_EKID_PART

    return sql to filtersClauses.second

}

/*
 * Creates a preparable query with the following clauses.
 */
private fun buildPreparableFiltersClause(
        startIndex: Int,
        propertyTypes: Map<UUID, PropertyType>,
        propertyTypeFilters: Map<UUID, Set<Filter>>
): Pair<String, Set<SqlBinder>> {
    val bindList = propertyTypeFilters.entries
            .filter { (_, filters) -> filters.isEmpty() }
            .flatMap { (propertyTypeId, filters) ->
                val nCol = PostgresDataTables
                        .nonIndexedValueColumn(
                                PostgresEdmTypeConverter.map(propertyTypes.getValue(propertyTypeId).datatype)
                        )
                val bCol = PostgresDataTables
                        .btreeIndexedValueColumn(
                                PostgresEdmTypeConverter.map(propertyTypes.getValue(propertyTypeId).datatype)
                        )

                //Generate sql preparable sql fragments
                var currentIndex = startIndex
                val nFilterFragments = filters.map { filter ->
                    val bindDetails = buildBindDetails(currentIndex, propertyTypeId, filter, nCol.name)
                    currentIndex = bindDetails.nextIndex
                    bindDetails
                }

                val bFilterFragments = filters
                        .map { filter ->
                            val bindDetails = buildBindDetails(currentIndex, propertyTypeId, filter, bCol.name)
                            currentIndex = bindDetails.nextIndex
                            bindDetails
                        }

                nFilterFragments + bFilterFragments
            }

    val sql = bindList.joinToString(" AND ") { "(${it.sql})" }
    val bindInfo = bindList.flatMap { it.bindInfo }.toSet()
    return sql to bindInfo
}

private fun buildBindDetails(
        startIndex: Int,
        propertyTypeId: UUID,
        filter: Filter,
        col: String
): BindDetails {
    val bindInfo = linkedSetOf<SqlBinder>()
    bindInfo.add(SqlBinder(SqlBindInfo(startIndex, propertyTypeId), ::doBind))
    bindInfo.addAll(filter.bindInfo(startIndex).map { SqlBinder(it, ::doBind) })
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

internal val GROUP_BY_ESID_EKID_PART_PTID = "GROUP BY (${ENTITY_SET_ID.name},${ID_VALUE.name}, ${PARTITION.name}, ${PROPERTY_TYPE_ID.name})"
internal val GROUP_BY_ESID_EKID_PART = "GROUP BY (${ENTITY_SET_ID.name},${ID_VALUE.name}, ${PARTITION.name})"
internal val GROUP_BY_ID_PT_ID_HASH = "GROUP BY (${ID_VALUE.name}, ${PARTITION.name}, ${PROPERTY_TYPE_ID.name}, ${HASH.name})"
internal val GROUP_BY_ID = "GROUP BY (${ID_VALUE.name}, ${PARTITION.name})"
//TODO: This was in case we needed to filter entity_set_ids, but we don't since for entity_set_id to show up it must
//be in at least one (id, pt_id, hash) pair
//internal val DATA_COLULMNS_NOT_NULL = PostgresDataTables.dataTableValueColumns.joinToString(" AND ") {
//  "${it.name} IS NOT NULL"
//}
//internal val FILTERED_ENTITY_SET_IDS = "array_agg(${ENTITY_SET_ID.name}) FILTER ( ) as ${ENTITY_SET_IDS.name}"


/**
 * Preparable SQL that selects entities across multiple entity sets grouping by id and property type id from the [DATA]
 * table with the following bind order:
 *
 * 1. entity set ids (array)
 * 2. entity key ids (array)
 * 3. partition (array)
 *
 */
internal fun selectEntitiesGroupedByIdAndPropertyTypeId(
        idsPresent: Boolean = true,
        partitionsPresent: Boolean = true,
        entitySetsPresent: Boolean = true
): String {

    val entitySetClause = if (entitySetsPresent) "${ENTITY_SET_ID.name} = ANY(?)" else ""
    val idsClause = if (idsPresent) "${ID_VALUE.name} = ANY(?)" else ""
    val partitionClause = if (partitionsPresent) "${PARTITION.name} = ANY(?)" else ""
    val versionsClause = "${VERSION.name} > 0 "

    val optionalClauses = listOf(entitySetClause, idsClause, partitionClause, versionsClause).filter { it.isNotBlank() }
    val optionalClausesSql = if (optionalClauses.isEmpty()) "" else "WHERE ${optionalClauses.joinToString(" AND ")}"

    return "SELECT ${ENTITY_SET_ID.name},${ID_VALUE.name},${PARTITION.name},${PROPERTY_TYPE_ID.name},$valuesColumnsSql " +
            "FROM ${DATA.name} $optionalClausesSql"
}


//"GROUP BY (${ENTITY_SET_ID.name},${ID_VALUE.name}, ${PARTITION.name}, ${PROPERTY_TYPE_ID.name})"
/**
 * Preparable SQL that selects entire entity sets grouping by id and property type id from the [DATA] table with the
 * following bind order:
 *
 * 1. entity set ids (array)
 *
 */
internal val selectEntitySetGroupedByIdAndPropertyTypeId =
        "SELECT ${ENTITY_SET_ID.name},${ID_VALUE.name},${PARTITION.name},${PROPERTY_TYPE_ID.name},$valuesColumnsSql " +
                "FROM ${DATA.name} WHERE ${ENTITY_SET_ID.name} = ANY(?) "

/**
 * Preparable SQL that selects entities across multiple entity sets grouping by id and property type id from the [DATA]
 * table with the following bind order:
 *
 * 1. entity set ids (array)
 * 2. entity key ids (array)
 * 3. partition (array)
 *
 */
internal val selectEntitiesSql =
        "SELECT ${ENTITY_SET_ID.name},${ID_VALUE.name},$jsonValueColumnsSql FROM (${selectEntitiesGroupedByIdAndPropertyTypeId()}) entities " +
                GROUP_BY_ESID_EKID_PART

/**
 * Preparable SQL that selects entire entity sets grouping by id and property type id from the [DATA] table with the
 * following bind order:
 *
 * 1. entity set ids (array)
 *
 */
internal val selectEntitySetsSql =
        "SELECT ${ENTITY_SET_ID.name},${ID_VALUE.name},$jsonValueColumnsSql FROM ($selectEntitySetGroupedByIdAndPropertyTypeId) entity_set " +
                GROUP_BY_ESID_EKID_PART


/**
 * Preparable SQL that selects linking entities across multiple entity sets grouping by linking id and property type id
 * from the  [DATA] table with the following bind order:
 *
 * 1. normal entity set ids (array)
 * 2. linking ids (array)
 * 3. partition (array)
 *
 */
internal val selectLinkingEntitiesByNormalEntitySetIdsSql =
        "SELECT ${ENTITY_SET_ID.name},${LINKING_ID.name},$jsonValueColumnsSql FROM (${selectEntitiesGroupedByIdAndPropertyTypeId()} entities " +
                "GROUP BY (${ENTITY_SET_ID.name},${LINKING_ID.name}, ${PARTITION.name})"

/**
 * Preparable SQL that selects linking entities across multiple entity sets grouping by linking id and property type id
 * from the  [DATA] table with the following bind order:
 *
 * 1. normal entity set ids (array)
 * 2. linking ids (array)
 * 3. partition (array)
 *
 * Note: It's only selecting from 1 linking entity set
 *
 */
// todo: what if we want to select multiple linking entity sets?
internal fun selectLinkingEntitiesByLinkingEntitySetIdSql(linkingEntitySetId: UUID): String {
    return "SELECT '$linkingEntitySetId' AS ${ENTITY_SET_ID.name},${LINKING_ID.name},$jsonValueColumnsSql FROM (${selectEntitiesGroupedByIdAndPropertyTypeId()}) entities " +
            "GROUP BY (${ENTITY_SET_ID.name},${LINKING_ID.name}, ${PARTITION.name})"
}

/**
 * Preparable SQL that selects an entire linking entity set grouping by linking id and property type id from the [DATA]
 * table with the following bind order:
 *
 * 1. normal entity set ids (array)
 *
 * Note: It's only selecting from 1 linking entity set
 *
 */
// todo: what if we want to select multiple linking entity sets?
internal fun selectLinkingEntitySetSql(linkingEntitySetId: UUID): String {
    return "SELECT '$linkingEntitySetId' AS ${ENTITY_SET_ID.name},${ID.name},$jsonValueColumnsSql FROM ($selectEntitySetGroupedByIdAndPropertyTypeId) entity_set " +
            "GROUP BY (${ENTITY_SET_ID.name},${ID.name}, ${PARTITION.name})"
}

/**
 * 1 - versions
 * 2 - version
 * 3 - version
 * 4 - entity set id
 * 5 - entity key ids
 */
internal val upsertEntitiesSql = "UPDATE ${IDS.name} SET ${VERSIONS.name} = ${VERSIONS.name} || ?, ${DataTables.LAST_WRITE.name} = now(), " +
        "${VERSION.name} = CASE WHEN abs(${IDS.name}.${VERSION.name}) < ? THEN ? " +
        "ELSE ${IDS.name}.${VERSION.name} END " +
        "WHERE ${ENTITY_SET_ID.name} = ? AND ${ID_VALUE.name} = ANY(?)"


/**
 * Preparable sql to lock entities with the following bind order:
 * 1. entity key ids
 * 2. partitions
 */
internal val lockEntitiesSql = "SELECT 1 FROM ${IDS.name} " +
        "WHERE ${ID_VALUE.name} = ANY(?) AND ${PARTITION.name} = ANY(?) " +
        "FOR UPDATE"

/**
 * Preparable SQL that upserts a version for all entities in a given entity set in [IDS]
 *
 * The following bind order is expected:
 *
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 */
internal val updateVersionsForEntitySet = "UPDATE ${IDS.name} SET versions = versions || ARRAY[?], " +
        "${VERSION.name} = CASE WHEN abs(${IDS.name}.${VERSION.name}) < abs(?) THEN ? " +
        "ELSE ${IDS.name}.${VERSION.name} END " +
        "WHERE ${ENTITY_SET_ID.name} = ? "

/**
 * Preparable SQL that updates a version for all properties in a given entity set in [DATA]
 *
 * The following bind order is expected:
 *
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 */
internal val updateVersionsForPropertiesInEntitySet = "UPDATE ${DATA.name} SET versions = versions || ARRAY[?], " +
        "${VERSION.name} = CASE WHEN abs(${DATA.name}.${VERSION.name}) < abs(?) THEN ? " +
        "ELSE ${DATA.name}.${VERSION.name} END " +
        "WHERE ${ENTITY_SET_ID.name} = ? "


/**
 * Preparable SQL that upserts a version for all entities in a given entity set in [IDS]
 *
 * The following bind order is expected:
 *
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 * 5. entity key ids
 * 6. partition
 * 7. partition version
 */
internal val updateVersionsForEntitiesInEntitySet = "$updateVersionsForEntitySet AND ${ID_VALUE.name} = ANY(?) " +
        "AND ${PARTITION.name} = ANY(?) AND ${PARTITIONS_VERSION.name} = ?"

/**
 * Preparable SQL thatupserts a version for all properties in a given entity set in [PostgresTable.DATA]
 *
 * The following bind order is expected:
 *
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 * 5. property type ids
 */
internal val updateVersionsForPropertyTypesInEntitySet = "$updateVersionsForPropertiesInEntitySet AND ${PROPERTY_TYPE_ID.name} = ANY(?)"

/**
 * Preparable SQL that updates a version for all properties in a given entity set in [PostgresTable.DATA]
 *
 * The following bind order is expected:
 *
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 * 5. entity key ids
 * 6. partition
 * 7. partition version
 */
internal val updateVersionsForPropertiesInEntitiesInEntitySet = "$updateVersionsForPropertiesInEntitySet AND ${ID_VALUE.name} = ANY(?) " +
        "AND PARTITION = ANY(?) AND ${PARTITIONS_VERSION.name} = ? "

/**
 * Preparable SQL thatpserts a version for all properties in a given entity set in [PostgresTable.DATA]
 *
 * The following bind order is expected:
 *
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 * 5. entity key ids
 * 6. partition
 * 7. partition version
 * 8. property type ids
 */
internal val updateVersionsForPropertyTypesInEntitiesInEntitySet = "$updateVersionsForPropertiesInEntitiesInEntitySet AND ${PROPERTY_TYPE_ID.name} = ANY(?)"

/**
 * Preparable SQL updates a version for all property values in a given entity set in [PostgresTable.DATA]
 *
 * The following bind order is expected:
 *
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 * 5. entity key ids
 * 6. partition
 * 7. partition version
 * 8. property type id
 * 9. value
 */
internal val updateVersionsForPropertyValuesInEntitiesInEntitySet = "$updateVersionsForPropertiesInEntitySet AND ${ID_VALUE.name} = ANY(?) " +
        "AND PARTITION = ANY(?) AND ${PARTITIONS_VERSION.name} = ? ${PROPERTY_TYPE_ID.name} = ? AND ${HASH.name} = ?"

/**
 * Selects a text properties from entity sets with the following bind order:
 * 1. entity set ids  (array)
 * 2. property type ids (array)
 *
 */
internal val selectEntitySetTextProperties = "SELECT COALESCE(${getSourceDataColumnName(PostgresDatatype.TEXT,IndexType.NONE)},${getSourceDataColumnName(PostgresDatatype.TEXT,IndexType.BTREE)}) AS ${getMergedDataColumnName(PostgresDatatype.TEXT)} " +
        "FROM ${DATA.name} " +
        "WHERE (${getSourceDataColumnName(PostgresDatatype.TEXT,IndexType.NONE)} IS NOT NULL OR ${getSourceDataColumnName(PostgresDatatype.TEXT,IndexType.BTREE)} IS NOT NULL) AND " +
        "${ENTITY_SET_ID.name} = ANY(?) AND ${PROPERTY_TYPE_ID.name} = ANY(?) "


/**
 * Selects a text properties from specific entities with the following bind order:
 * 1. entity set ids  (array)
 * 2. property type ids (array)
 * 3. entity key ids (array)
 */
internal val selectEntitiesTextProperties = "$selectEntitySetTextProperties AND ${ID_VALUE.name} = ANY(?)"

fun partitionSelectorFromId(entityKeyId: UUID): Int {
    return entityKeyId.leastSignificantBits.toInt()
}

fun getPartition(entityKeyId: UUID, partitions: List<Int>): Int {
    return partitions[partitionSelectorFromId(entityKeyId) % partitions.size]
}

/**
 * Builds the list of partitions for a given set of entity key ids.
 * @param entityKeyIds The entity key ids whose partitions will be retrieved.
 * @param partitions The partitions to select from.
 * @return A list of partitions.
 */
fun getPartitionsInfo(entityKeyIds: Set<UUID>, partitions: List<Int>): List<Int> {
    return entityKeyIds.map { entityKeyId -> getPartition(entityKeyId, partitions) }
}

/**
 * Builds a mapping of entity key id to partition.
 *
 * @param entityKeyIds The entity key ids whose partitions will be retrieved.
 * @param partitions The partitions to select from.
 *
 * @return A map of entity key ids to partitions.
 */
fun getPartitionsInfoMap(entityKeyIds: Set<UUID>, partitions: List<Int>): Map<UUID, Int> {
    return entityKeyIds.associateWith { entityKeyId -> getPartition(entityKeyId, partitions) }
}

fun getSourceDataColumnName(datatype: PostgresDatatype, indexType: IndexType) : String {
    return when (indexType) {
        IndexType.BTREE -> "b_${datatype.name}"
        IndexType.NONE -> "n_${datatype.name}"
        else -> throw IllegalStateException("Unsupported index type: $indexType")
    }
}

fun getMergedDataColumnName(datatype: PostgresDatatype): String {
    return "v_${datatype.name}"
}


/**
 * This function generates preparable sql with the following bind order:
 *
 * 1.  ENTITY_SET_ID
 * 2.  ID_VALUE
 * 3.  PARTITION
 * 4.  PROPERTY_TYPE_ID
 * 5.  HASH
 * 6.  LAST_WRITE
 * 7.  VERSION,
 * 8.  VERSIONS
 * 9.  PARTITIONS_VERSION
 * 10. Value Column
 */
fun upsertPropertyValueSql(propertyType: PropertyType): String {
    val insertColumn = getColumnDefinition(propertyType.postgresIndexType, propertyType.datatype)
    val metadataColumnsSql = listOf(
            ENTITY_SET_ID,
            ID_VALUE,
            PARTITION,
            PROPERTY_TYPE_ID,
            HASH,
            LAST_WRITE,
            VERSION,
            VERSIONS,
            PARTITIONS_VERSION
    ).joinToString(",") { it.name }
    return "INSERT INTO ${DATA.name} ($metadataColumnsSql,${insertColumn.name}) VALUES (?,?,?,?,?,now(),?,?,?,?) " +
            "ON CONFLICT (${PARTITION.name},${ENTITY_SET_ID.name},${PROPERTY_TYPE_ID.name},${ID_VALUE.name}, ${HASH.name}) DO UPDATE " +
            "SET ${VERSIONS.name} = ${DATA.name}.${VERSIONS.name} || EXCLUDED.${VERSIONS.name}, " +
            "${LAST_WRITE.name} = GREATEST(${DATA.name}.${LAST_WRITE.name},EXCLUDED.${LAST_WRITE.name}), " +
            "${PARTITIONS_VERSION.name} = EXCLUDED.${PARTITIONS_VERSION.name}, " +
            "${VERSION.name} = CASE WHEN abs(${DATA.name}.${VERSION.name}) < EXCLUDED.${VERSION.name} THEN EXCLUDED.${VERSION.name} " +
            "ELSE ${DATA.name}.${VERSION.name} END"
}



