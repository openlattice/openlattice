package com.openlattice.data.storage


import com.openlattice.IdConstants
import com.openlattice.analysis.SqlBindInfo
import com.openlattice.analysis.requests.Filter
import com.openlattice.edm.PostgresEdmTypeConverter
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.*
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresDataTables.Companion.getColumnDefinition
import com.openlattice.postgres.PostgresDataTables.Companion.getSourceDataColumnName
import com.openlattice.postgres.PostgresTable.*
import java.sql.PreparedStatement
import java.util.*

/**
 * This class is responsible for generating all the SQL for creating, reading, upated, and deleting entities.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
internal class PostgresDataQueries

const val VALUES = "values"


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
fun buildPreparableFiltersSql(
        startIndex: Int,
        propertyTypes: Map<UUID, PropertyType>,
        propertyTypeFilters: Map<UUID, Set<Filter>>,
        metadataOptions: Set<MetadataOption>,
        linking: Boolean,
        idsPresent: Boolean,
        partitionsPresent: Boolean
): Pair<String, Set<SqlBinder>> {
    val filtersClauses = buildPreparableFiltersClause(startIndex, propertyTypes, propertyTypeFilters)
    val filtersClause = if (filtersClauses.first.isNotEmpty()) " AND ${filtersClauses.first} " else ""

    val metadataOptionColumns = metadataOptions.associateWith(::mapMetaDataToColumnSql)
    val nonAggregatedMetadataSql = metadataOptionColumns
            .filter { !isMetaDataAggregated(it.key) }.values.joinToString("")
    val innerGroupBy = groupBy(ESID_EKID_PART_PTID + nonAggregatedMetadataSql)
    // TODO: remove IS NOT NULL post-migration
    val linkingClause = if (linking) {
        " AND ${ORIGIN_ID.name} IS NOT NULL AND ${ORIGIN_ID.name} != '${IdConstants.EMPTY_ORIGIN_ID.id}' "
    } else {
        ""
    }

    val innerSql = selectEntitiesGroupedByIdAndPropertyTypeId(
            metadataOptions,
            idsPresent = idsPresent,
            partitionsPresent = partitionsPresent
    ) + linkingClause + filtersClause + innerGroupBy

    val metadataOptionColumnsSql = metadataOptionColumns.values.joinToString("")
    val outerGroupBy = groupBy(ESID_EKID_PART + metadataOptionColumnsSql)
    val sql = "SELECT ${ENTITY_SET_ID.name},${ID_VALUE.name},${PARTITION.name}$metadataOptionColumnsSql,$jsonValueColumnsSql " +
            "FROM ($innerSql) entities $outerGroupBy"

    return sql to filtersClauses.second

}

internal fun selectEntitiesGroupedByIdAndPropertyTypeId(
        metadataOptions: Set<MetadataOption>,
        idsPresent: Boolean = true,
        partitionsPresent: Boolean = true,
        entitySetsPresent: Boolean = true
): String {
    val metadataOptionsSql = metadataOptions
            .joinToString("") { mapMetaDataToSelector(it) } // they already have the comma prefix
    return "SELECT ${ENTITY_SET_ID.name},${ID_VALUE.name},${PARTITION.name},${PROPERTY_TYPE_ID.name}$metadataOptionsSql,$valuesColumnsSql " +
            "FROM ${DATA.name} ${optionalWhereClauses(idsPresent, partitionsPresent, entitySetsPresent)}"
}

/**
 * Returns the correspondent column name used for the metadata option with a comma prefix.
 */
private fun mapMetaDataToColumnSql(metadataOption: MetadataOption): String {
    return ",${mapMetaDataToColumnName(metadataOption)}"
}

/**
 * Returns the correspondent column name used for the metadata option.
 */
private fun mapMetaDataToColumnName(metadataOption: MetadataOption): String {
    return when (metadataOption) {
        MetadataOption.ORIGIN_IDS -> ORIGIN_ID.name
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
        MetadataOption.ORIGIN_IDS -> ",${ORIGIN_ID.name}"
        MetadataOption.LAST_WRITE -> ",max(${LAST_WRITE.name}) AS ${mapMetaDataToColumnName(metadataOption)}"
        MetadataOption.ENTITY_KEY_IDS ->
            ",array_agg(COALESCE(${ORIGIN_ID.name},${ID.name})) AS ${mapMetaDataToColumnName(metadataOption)}"
        else -> throw UnsupportedOperationException("No implementation yet for metadata option $metadataOption")
    }
}

private fun isMetaDataAggregated(metadataOption: MetadataOption): Boolean {
    return when (metadataOption) {
        MetadataOption.ORIGIN_IDS -> false
        MetadataOption.LAST_WRITE -> true
        MetadataOption.ENTITY_KEY_IDS -> true
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

internal val ESID_EKID_PART = "${ENTITY_SET_ID.name},${ID_VALUE.name},${PARTITION.name}"
internal val ESID_EKID_PART_PTID = "${ENTITY_SET_ID.name},${ID_VALUE.name}, ${PARTITION.name},${PROPERTY_TYPE_ID.name}"

internal fun groupBy(columns: String): String {
    return "GROUP BY ($columns)"
}

/**
 * Preparable SQL that selects entities across multiple entity sets grouping by id and property type id from the [DATA]
 * table with the following bind order:
 *
 * 1. entity set ids (array)
 * 2. entity key ids (array)
 * 3. partition (array)
 *
 */

fun optionalWhereClauses(
        idsPresent: Boolean = true,
        partitionsPresent: Boolean = true,
        entitySetsPresent: Boolean = true
): String {
    val entitySetClause = if (entitySetsPresent) "${ENTITY_SET_ID.name} = ANY(?)" else ""
    val idsClause = if (idsPresent) "${ID_VALUE.name} = ANY(?)" else ""
    val partitionClause = if (partitionsPresent) "${PARTITION.name} = ANY(?)" else ""
    val versionsClause = "${VERSION.name} > 0 "

    val optionalClauses = listOf(entitySetClause, idsClause, partitionClause, versionsClause).filter { it.isNotBlank() }
    return "WHERE ${optionalClauses.joinToString(" AND ")}"
}


/**
 * 1 - versions
 * 2 - version
 * 3 - version
 * 4 - entity set id
 * 5 - entity key ids
 * 6 - partitions
 */
// @formatter:off
internal val upsertEntitiesSql = "UPDATE ${IDS.name} " +
        "SET ${VERSIONS.name} = ${VERSIONS.name} || ?, " +
            "${DataTables.LAST_WRITE.name} = now(), " +
            "${VERSION.name} = CASE " +
                "WHEN abs(${IDS.name}.${VERSION.name}) < abs(?) THEN ? " +
                "ELSE ${IDS.name}.${VERSION.name} " +
            "END " +
        "WHERE ${ENTITY_SET_ID.name} = ? AND ${ID_VALUE.name} = ANY(?) AND ${PARTITION.name} = ?"
// @formatter:on

/**
 * Preparable sql to lock entities with the following bind order:
 * 1. entity key ids
 * 2. partitions
 */
internal val lockEntitiesSql = "SELECT 1 FROM ${IDS.name} " +
        "WHERE ${ID_VALUE.name} = ANY(?) AND ${PARTITION.name} = ? " +
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
 * Preparable SQL thatupserts a version for all properties in a given entity set in [DATA]
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
 * Preparable SQL that updates a version for all properties in a given entity set in [DATA]
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
        "AND ${PARTITION.name} = ANY(?) AND ${PARTITIONS_VERSION.name} = ? "

/**
 * Preparable SQL thatpserts a version for all properties in a given entity set in [DATA]
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
 * Preparable SQL updates a version for all property values in a given entity set in [DATA]
 *
 * The following bind order is expected:
 *
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 * 5. entity key ids
 * 6. partitions
 * 7. partition version
 * 8. property type id
 * 9. value
 */
internal val updateVersionsForPropertyValuesInEntitiesInEntitySet = "$updateVersionsForPropertiesInEntitySet AND ${ID_VALUE.name} = ANY(?) " +
        "AND ${PARTITION.name} = ANY(?) AND ${PARTITIONS_VERSION.name} = ? ${PROPERTY_TYPE_ID.name} = ? AND ${HASH.name} = ?"


/**
 * Preparable SQL deletes a given property in a given entity set in [IDS]
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
 * Preparable SQL deletes all property values of entities in a given entity set in [DATA]
 *
 * The following bind order is expected:
 *
 * 1. entity set id
 * 2. entity key ids
 * 3. partition
 * 4. partition version
 * 5. property type ids
 */
internal val deletePropertiesOfEntitiesInEntitySet = "DELETE FROM ${DATA.name} " +
        "WHERE ${ENTITY_SET_ID.name} = ? AND ${ID_VALUE.name} = ANY(?) AND ${PARTITION.name} = ? AND ${PARTITIONS_VERSION.name} = ? AND ${PROPERTY_TYPE_ID.name} = ANY(?) "

/**
 * Preparable SQL deletes all property values of entities and entity key id in a given entity set in [DATA]
 *
 * The following bind order is expected:
 *
 * 1. entity set id
 * 2. entity key ids
 * 3. partition
 * 4. partition version
 */
internal val deleteEntitiesInEntitySet = "DELETE FROM ${DATA.name} " +
        "WHERE ${ENTITY_SET_ID.name} = ? AND ${ID_VALUE.name} = ANY(?) AND ${PARTITION.name} = ? AND ${PARTITIONS_VERSION.name} = ? "

/**
 * Preparable SQL updates last write to current time for all entity ids a given entity set in [IDS]
 *
 * The following bind order is expected:
 *
 * 1. entity set id
 * 2. entity key ids
 * 3. partition
 * 4. partition version
 */
internal val updateLastWriteForEntitiesInEntitySet = "UPDATE ${IDS.name} SET ${LAST_WRITE.name} = 'now()' " +
        "WHERE ${ENTITY_SET_ID.name} = ? AND ${ID.name} = ANY(?) AND ${PARTITION.name} = ANY(?) AND ${PARTITIONS_VERSION.name} = ?"

/**
 * Preparable SQL deletes all entities in a given entity set in [IDS]
 *
 * The following bind order is expected:
 *
 * 1. entity set id
 * 2. entity key ids
 */
internal val deleteEntityKeys = "DELETE FROM ${IDS.name} WHERE ${ENTITY_SET_ID.name} = ? AND ${ID.name} = ANY(?)"

/**
 * Selects a text properties from entity sets with the following bind order:
 * 1. entity set ids  (array)
 * 2. property type ids (array)
 *
 */
internal val selectEntitySetTextProperties = "SELECT COALESCE(${getSourceDataColumnName(PostgresDatatype.TEXT, IndexType.NONE)},${getSourceDataColumnName(PostgresDatatype.TEXT, IndexType.BTREE)}) AS ${getMergedDataColumnName(PostgresDatatype.TEXT)} " +
        "FROM ${DATA.name} " +
        "WHERE (${getSourceDataColumnName(PostgresDatatype.TEXT, IndexType.NONE)} IS NOT NULL OR ${getSourceDataColumnName(PostgresDatatype.TEXT, IndexType.BTREE)} IS NOT NULL) AND " +
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
            "ON CONFLICT (${PARTITION.name},${ENTITY_SET_ID.name},${PROPERTY_TYPE_ID.name},${ID_VALUE.name}, ${HASH.name}, ${PARTITIONS_VERSION.name}) DO UPDATE " +
            "SET ${VERSIONS.name} = ${DATA.name}.${VERSIONS.name} || EXCLUDED.${VERSIONS.name}, " +
            "${LAST_WRITE.name} = GREATEST(${DATA.name}.${LAST_WRITE.name},EXCLUDED.${LAST_WRITE.name}), " +
            "${PARTITIONS_VERSION.name} = EXCLUDED.${PARTITIONS_VERSION.name}, " +
            "${VERSION.name} = CASE WHEN abs(${DATA.name}.${VERSION.name}) < EXCLUDED.${VERSION.name} THEN EXCLUDED.${VERSION.name} " +
            "ELSE ${DATA.name}.${VERSION.name} END"
}


/* For materialized views */

/**
 * This function generates preparable sql for selecting property values columnar for a given entity set.
 * Bind order is the following:
 * 1. entity set ids (uuid array)
 * 2. partitions (int array)
 */
fun selectPropertyTypesOfEntitySetColumnar(
        authorizedPropertyTypes: Map<UUID, PropertyType>,
        linking: Boolean
): String {
    val idColumnsList = listOf(ENTITY_SET_ID.name, ID.name, ENTITY_KEY_IDS_COL.name)
    val (entitySetData, _) = buildPreparableFiltersSql(
            3,
            authorizedPropertyTypes,
            mapOf(),
            EnumSet.of(MetadataOption.ENTITY_KEY_IDS),
            linking,
            idsPresent = false,
            partitionsPresent = true
    )

    val selectColumns = (idColumnsList +
            (authorizedPropertyTypes.map { selectPropertyColumn(it.value) }))
            .joinToString()
    val groupByColumns = idColumnsList.joinToString()
    val selectArrayColumns = (idColumnsList +
            (authorizedPropertyTypes.map { selectPropertyArray(it.value) }))
            .joinToString()

    return "SELECT $selectArrayColumns FROM (SELECT $selectColumns FROM ($entitySetData) as entity_set_data) as grouped_data GROUP BY ($groupByColumns)"
}


private fun selectPropertyColumn(propertyType: PropertyType): String {
    val dataType = PostgresEdmTypeConverter.map(propertyType.datatype)
    val mergedName = getMergedDataColumnName(dataType)
    val propertyColumnName = propertyColumnName(propertyType)

    return "jsonb_array_elements_text($mergedName -> '${propertyType.id}') AS $propertyColumnName"
}

private fun selectPropertyArray(propertyType: PropertyType): String {
    val propertyColumnName = propertyColumnName(propertyType)
    return "array_agg($propertyColumnName) FILTER (WHERE $propertyColumnName IS NOT NULL) as $propertyColumnName"
}

private fun propertyColumnName(propertyType: PropertyType): String {
    return DataTables.quote(propertyType.type.fullQualifiedNameAsString)
}
