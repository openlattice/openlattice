package com.openlattice.data.storage


import com.openlattice.edm.PostgresEdmTypeConverter
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.*
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.DATA
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import java.util.*

/**
 * This class is responsible for generating all the SQL for creating, reading, upated, and deleting entities.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
internal class PostgresDataQueries

val dataMetadataColumnsParametersSql = PostgresDataTables.dataTableMetadataColumns.joinToString(",") { "?" }
val dataMetadataColumnsSql = PostgresDataTables.dataTableMetadataColumns.joinToString { "," }
val dataTableColumnsSql = PostgresDataTables.dataTableColumns.joinToString(",") { it.name }
val dataTableColumnsBindSql = PostgresDataTables.dataTableColumns.joinToString(",") { "?" }
val dataTableColumnsConflictSetSql = PostgresDataTables.dataTableColumns.joinToString(
        ","
) { "${it.name} = EXCLUDED.${it.name}" }

/**
 * 1 - version
 * 2 - version
 * 3 - version
 * 4 - entity set id
 * 5 - entity key ids
 */
internal val upsertEntitiesSql = "UPDATE ${PostgresTable.ENTITY_KEY_IDS.name} SET ${VERSIONS.name} = ${VERSIONS.name} || ARRAY[?], ${DataTables.LAST_WRITE.name} = now(), " +
        "${VERSION.name} = CASE WHEN abs(${PostgresTable.ENTITY_KEY_IDS.name}.${VERSION.name}) < ? THEN $ " +
        "ELSE ${PostgresTable.ENTITY_KEY_IDS.name}.${VERSION.name} END " +
        "WHERE ${ENTITY_SET_ID.name} = ? AND ${ID_VALUE.name} = ANY(?)"


/**
 * This function generates preparable sql with the following bind order:
 * 1. entity set id
 * 2. entity key ids
 * 3. partition
 */
internal val lockEntitiesSql = "SELECT 1 FROM ${PostgresTable.ENTITY_KEY_IDS.name} " +
        "WHERE ${ENTITY_SET_ID.name} = ? AND ${ID_VALUE.name} = ANY(?) AND $PARTITION = ANY(?) " +
        "FOR UPDATE"


fun upsertEntities(entitySetId: UUID, idsClause: String, version: Long): String {
    return "UPDATE ${PostgresTable.ENTITY_KEY_IDS.name} SET ${VERSIONS.name} = ${VERSIONS.name} || ARRAY[$version], ${DataTables.LAST_WRITE.name} = now(), " +
            "${VERSION.name} = CASE WHEN abs(${PostgresTable.ENTITY_KEY_IDS.name}.${VERSION.name}) < $version THEN $version " +
            "ELSE ${PostgresTable.ENTITY_KEY_IDS.name}.${VERSION.name} END " +
            "WHERE ${ENTITY_SET_ID.name} = '$entitySetId' AND ${ID_VALUE.name} IN ($idsClause)"
}

/**
 * Prepared statement for that upserts a version for all entities in a given entity set in [PostgresTable.ENTITY_KEY_IDS]
 *
 * The following bind order is expected:
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 */
internal val updateVersionsForEntitySet = "UPDATE ${PostgresTable.ENTITY_KEY_IDS.name} SET versions = versions || ARRAY[?]::uuid[], " +
        "${VERSION.name} = CASE WHEN abs(${PostgresTable.ENTITY_KEY_IDS.name}.${VERSION.name}) < ? THEN ? " +
        "ELSE ${PostgresTable.ENTITY_KEY_IDS.name}.${VERSION.name} END " +
        "WHERE ${ENTITY_SET_ID.name} = ? "

/**
 * Prepared statement for that upserts a version for all properties in a given entity set in [PostgresTable.DATA]
 *
 * The following bind order is expected:
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 */
internal val updateVersionsForPropertiesInEntitySet = "UPDATE ${DATA.name} SET versions = versions || ARRAY[?]::uuid[], " +
        "${VERSION.name} = CASE WHEN abs(${PostgresTable.ENTITY_KEY_IDS.name}.${VERSION.name}) < ? THEN ? " +
        "ELSE ${PostgresTable.ENTITY_KEY_IDS.name}.${VERSION.name} END " +
        "WHERE ${ENTITY_SET_ID.name} = ? "


/**
 * Prepared statement for that upserts a version for all entities in a given entity set in [PostgresTable.ENTITY_KEY_IDS]
 *
 * The following bind order is expected:
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 * 5. entity key ids
 * 6. partition
 */
internal val updateVersionsForEntitiesInEntitySet = "$updateVersionsForEntitySet AND ${ID_VALUE.name} = ANY(?) " +
        "AND PARTITION = ANY(?)"

/**
 * Prepared statement for that upserts a version for all properties in a given entity set in [PostgresTable.DATA]
 *
 * The following bind order is expected:
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 * 5. property type ids
 */
internal val updateVersionsForPropertyTypesInEntitySet = "$updateVersionsForPropertiesInEntitySet AND ${PROPERTY_TYPE_ID.name} = ANY(?)"

/**
 * Prepared statement for that upserts a version for all properties in a given entity set in [PostgresTable.DATA]
 *
 * The following bind order is expected:
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 * 5. entity key ids
 * 6. partition
 */
internal val updateVersionsForPropertiesInEntitiesInEntitySet = "$updateVersionsForPropertiesInEntitySet AND ${ID_VALUE.name} = ANY(?) " +
        "AND PARTITION = ANY(?)"
/**
 * Prepared statement for that upserts a version for all properties in a given entity set in [PostgresTable.DATA]
 *
 * The following bind order is expected:
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 * 5. entity key ids
 * 6. partition
 * 7. property type ids
 */
internal val updateVersionsForPropertyTypesInEntitiesInEntitySet = "$updateVersionsForPropertiesInEntitiesInEntitySet AND ${PROPERTY_TYPE_ID.name} = ANY(?)"



fun partitionSelectorFromId(entityKeyId: UUID): Int {
    return entityKeyId.leastSignificantBits.toInt()
}

fun getPartition(entityKeyId: UUID, partitions: List<Int>): Int {
    return partitions[partitionSelectorFromId(entityKeyId) % partitions.size]
}

/**
 * This function generates preparable sql with the following bind order:
 * 1.  ENTITY_SET_ID
 * 2.  ID_VALUE
 * 3.  PARTITION
 * 4.  PROPERTY_TYPE_ID
 * 5.  HASH
 * 6.  LAST_WRITE
 * 7.  LAST_PROPAGATE,
 * 8.  LAST_MIGRATE,
 * 9.  VERSION,
 * 10. VERSIONS
 * 11. Value Column
 */
fun upsertPropertyValueSql(propertyType: PropertyType): String {
    val insertColumn = getColumnDefinition(propertyType.postgresIndexType, propertyType.datatype)
    val metadataColumns = listOf(
            ENTITY_SET_ID,
            ID_VALUE,
            PARTITION,
            PROPERTY_TYPE_ID,
            HASH,
            LAST_WRITE,
            VERSION,
            VERSIONS
    )
    return "INSERT INTO ${DATA.name} ($dataMetadataColumnsSql,${insertColumn.name}) VALUES (?,?,?,?,?,now(),?,?,?) " +
            "ON CONFLICT (${ENTITY_SET_ID.name},${ID_VALUE.name}, ${HASH.name}) DO UPDATE " +
            "SET ${VERSIONS.name} = ${DATA.name}.${VERSIONS.name} || EXCLUDED.${VERSIONS.name}, " +
            "${LAST_WRITE.name} = GREATEST(${LAST_WRITE.name},EXCLUDED.${LAST_WRITE.name}), " +
            "${VERSION.name} = CASE WHEN abs(${DATA.name}.${VERSION.name}) < EXCLUDED.${VERSION.name} THEN EXCLUDED.${VERSION.name} " +
            "ELSE ${DATA.name}.${VERSION.name} END"
}

fun getColumnDefinition(indexType: IndexType, edmType: EdmPrimitiveTypeKind): PostgresColumnDefinition {
    return when (indexType) {
        IndexType.BTREE -> PostgresDataTables.btreeIndexedValueColumn(PostgresEdmTypeConverter.map(edmType))
        IndexType.GIN -> PostgresDataTables.ginIndexedValueColumn(PostgresEdmTypeConverter.map(edmType))
        IndexType.NONE -> PostgresDataTables.nonIndexedValueColumn(PostgresEdmTypeConverter.map(edmType))
        else -> throw IllegalArgumentException("HASH indexes are not yet supported by openlattice.")
    }
}
