package com.openlattice.data.storage

import com.openlattice.edm.PostgresEdmTypeConverter
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.*
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresTable.DATA
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import java.lang.IllegalArgumentException
import java.util.*

/**
 * This class is responsible for generating all the SQL for creating, reading, upated, and deleting entities.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
internal class PostgresDataQueries

val dataMetadataColumnsParametersSql = PostgresDataTables.dataTableMetadataColumns.joinToString(",") { "?" }
val dataMetadataColumnsSql = PostgresDataTables.dataTableMetadataColumns.joinToString { "," }
val dataTableColumnsSql = PostgresDataTables.dataTableColumns.joinToString(",")


/**
 * 1 - version
 * 2 - version
 * 3 - version
 * 4 - entity set id
 * 5 - entity key ids
 */
fun upsertEntitiesSql(): String {
    return "UPDATE ${PostgresTable.ENTITY_KEY_IDS.name} SET ${PostgresColumn.VERSIONS.name} = ${PostgresColumn.VERSIONS.name} || ARRAY[?], ${DataTables.LAST_WRITE.name} = now(), " +
            "${PostgresColumn.VERSION.name} = CASE WHEN abs(${PostgresTable.ENTITY_KEY_IDS.name}.${PostgresColumn.VERSION.name}) < ? THEN $ " +
            "ELSE ${PostgresTable.ENTITY_KEY_IDS.name}.${PostgresColumn.VERSION.name} END " +
            "WHERE ${PostgresColumn.ENTITY_SET_ID.name} = ? AND ${PostgresColumn.ID_VALUE.name} = ANY(?)"
}

/**
 * This function generates preparable sql with the following bind order:
 * 1. entity set id
 * 2. entity key ids
 * 3. partition
 */
fun lockEntitiesSql(): String {
    return "SELECT 1 FROM ${PostgresTable.ENTITY_KEY_IDS.name} " +
            "WHERE ${PostgresColumn.ENTITY_SET_ID.name} = ? AND ${PostgresColumn.ID_VALUE.name} = ANY(?) AND ${PostgresColumn.PARTITION} = ANY(?) " +
            "FOR UPDATE"
}

fun upsertEntities(entitySetId: UUID, idsClause: String, version: Long): String {
    return "UPDATE ${PostgresTable.ENTITY_KEY_IDS.name} SET ${PostgresColumn.VERSIONS.name} = ${PostgresColumn.VERSIONS.name} || ARRAY[$version], ${DataTables.LAST_WRITE.name} = now(), " +
            "${PostgresColumn.VERSION.name} = CASE WHEN abs(${PostgresTable.ENTITY_KEY_IDS.name}.${PostgresColumn.VERSION.name}) < $version THEN $version " +
            "ELSE ${PostgresTable.ENTITY_KEY_IDS.name}.${PostgresColumn.VERSION.name} END " +
            "WHERE ${PostgresColumn.ENTITY_SET_ID.name} = '$entitySetId' AND ${PostgresColumn.ID_VALUE.name} IN ($idsClause)"
}

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
            PostgresColumn.ENTITY_SET_ID,
            PostgresColumn.ID_VALUE,
            PostgresColumn.PARTITION,
            PostgresColumn.PROPERTY_TYPE_ID,
            PostgresColumn.HASH,
            LAST_WRITE,
            PostgresColumn.VERSION,
            PostgresColumn.VERSIONS
    )
    return "INSERT INTO ${DATA.name} ($dataMetadataColumnsSql,${insertColumn.name}) VALUES (?,?,?,?,?,now(),?,?,?) " +
            "ON CONFLICT (${PostgresColumn.ENTITY_SET_ID.name},${PostgresColumn.ID_VALUE.name}, ${PostgresColumn.HASH.name}) DO UPDATE " +
            "SET ${PostgresColumn.VERSIONS.name} = ${DATA.name}.${PostgresColumn.VERSIONS.name} || EXCLUDED.${PostgresColumn.VERSIONS.name}, " +
            "${LAST_WRITE.name} = GREATEST(${LAST_WRITE.name},EXCLUDED.${LAST_WRITE.name}), " +
            "${PostgresColumn.VERSION.name} = CASE WHEN abs(${DATA.name}.${PostgresColumn.VERSION.name}) < EXCLUDED.${PostgresColumn.VERSION.name} THEN EXCLUDED.${PostgresColumn.VERSION.name} " +
            "ELSE ${DATA.name}.${PostgresColumn.VERSION.name} END"
}

fun getColumnDefinition(indexType: IndexType, edmType: EdmPrimitiveTypeKind): PostgresColumnDefinition {
    return when (indexType) {
        IndexType.BTREE -> PostgresDataTables.btreeIndexedValueColumn(PostgresEdmTypeConverter.map(edmType))
        IndexType.GIN -> PostgresDataTables.ginIndexedValueColumn(PostgresEdmTypeConverter.map(edmType))
        IndexType.NONE -> PostgresDataTables.nonIndexedValueColumn(PostgresEdmTypeConverter.map(edmType))
        else -> throw IllegalArgumentException("HASH indexes are not yet supported by openlattice.")
    }
}