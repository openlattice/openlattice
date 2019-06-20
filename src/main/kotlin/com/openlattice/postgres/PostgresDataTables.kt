package com.openlattice.postgres

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.openlattice.edm.PostgresEdmTypeConverter
import com.openlattice.postgres.DataTables.*
import com.openlattice.postgres.PostgresColumn.*
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresDataTables {
    companion object {
        private val supportedEdmPrimitiveTypeKinds: Array<EdmPrimitiveTypeKind> = arrayOf(
                EdmPrimitiveTypeKind.String,
                EdmPrimitiveTypeKind.Guid,
                EdmPrimitiveTypeKind.Byte,
                EdmPrimitiveTypeKind.Int16,
                EdmPrimitiveTypeKind.Int32,
                EdmPrimitiveTypeKind.Duration,
                EdmPrimitiveTypeKind.Int64,
                EdmPrimitiveTypeKind.Date,
                EdmPrimitiveTypeKind.DateTimeOffset,
                EdmPrimitiveTypeKind.Double,
                EdmPrimitiveTypeKind.Boolean,
                EdmPrimitiveTypeKind.Binary
        )
        val nonIndexedColumns = supportedEdmPrimitiveTypeKinds
                .map(PostgresEdmTypeConverter::map)
                .map(::nonIndexedValueColumn)
        val btreeIndexedColumns = supportedEdmPrimitiveTypeKinds
                .map(PostgresEdmTypeConverter::map)
                .map(::btreeIndexedValueColumn)
        val ginIndexedColumns = supportedEdmPrimitiveTypeKinds
                .map(PostgresEdmTypeConverter::map)
                .map(::ginIndexedValueColumn)

        val dataTableMetadataColumns = listOf(
                ENTITY_SET_ID,
                ID_VALUE,
                PARTITION,
                PROPERTY_TYPE_ID,
                HASH,
                LAST_WRITE,
                LAST_PROPAGATE,
                LAST_MIGRATE,
                VERSION,
                VERSIONS,
                PARTITIONS_VERSION
        )
        val dataTableColumns = dataTableMetadataColumns + btreeIndexedColumns + ginIndexedColumns + nonIndexedColumns

        private val columnDefinitionCache = CacheBuilder.newBuilder().build(
                object : CacheLoader<Pair<IndexType, EdmPrimitiveTypeKind>, PostgresColumnDefinition>() {
                    override fun load(key: Pair<IndexType, EdmPrimitiveTypeKind>): PostgresColumnDefinition {
                        return buildColumnDefinition(key)
                    }

                    override fun loadAll(
                            keys: MutableIterable<Pair<IndexType, EdmPrimitiveTypeKind>>
                    ): MutableMap<Pair<IndexType, EdmPrimitiveTypeKind>, PostgresColumnDefinition> {
                        return keys.associateWith { buildColumnDefinition(it) }.toMutableMap()
                    }

                    private fun buildColumnDefinition(
                            key: Pair<IndexType, EdmPrimitiveTypeKind>
                    ): PostgresColumnDefinition {
                        val (indexType, edmType) = key
                        return when (indexType) {
                            IndexType.BTREE -> btreeIndexedValueColumn(
                                    PostgresEdmTypeConverter.map(edmType)
                            )
                            IndexType.GIN -> ginIndexedValueColumn(
                                    PostgresEdmTypeConverter.map(edmType)
                            )
                            IndexType.NONE -> nonIndexedValueColumn(
                                    PostgresEdmTypeConverter.map(edmType)
                            )
                            else -> throw IllegalArgumentException("HASH indexes are not yet supported by openlattice.")
                        }
                    }
                }
        )

        @JvmStatic
        fun buildDataTableDefinition(): PostgresTableDefinition {
            val columns = dataTableColumns.toTypedArray()

            val tableDefinition = CitusDistributedTableDefinition("data")
                    .addColumns(*columns)
                    .primaryKey(ID_VALUE, PARTITION, PROPERTY_TYPE_ID, HASH)
                    .distributionColumn(PARTITION)

            tableDefinition.addIndexes(
                    *btreeIndexedColumns.map { buildBtreeIndexDefinition(tableDefinition, it) }.toTypedArray()
            )

            tableDefinition.addIndexes(
                    *ginIndexedColumns.map { buildGinIndexDefinition(tableDefinition, it) }.toTypedArray()
            )

            return tableDefinition
        }

        @JvmStatic
        fun buildBtreeIndexDefinition(
                tableDefinition: PostgresTableDefinition,
                columnDefinition: PostgresColumnDefinition
        ): PostgresIndexDefinition {
            return PostgresColumnsIndexDefinition(tableDefinition, columnDefinition)
        }

        @JvmStatic
        fun buildGinIndexDefinition(
                tableDefinition: PostgresTableDefinition,
                columnDefinition: PostgresColumnDefinition
        ): PostgresIndexDefinition {
            return PostgresColumnsIndexDefinition(tableDefinition, columnDefinition).method(IndexType.GIN)
        }

        @JvmStatic
        fun nonIndexedValueColumn(datatype: PostgresDatatype): PostgresColumnDefinition {
            return PostgresColumnDefinition("n_${datatype.name}", datatype)
        }

        @JvmStatic
        fun ginIndexedValueColumn(datatype: PostgresDatatype): PostgresColumnDefinition {
            return PostgresColumnDefinition("g_${datatype.name}", datatype)
        }

        @JvmStatic
        fun btreeIndexedValueColumn(datatype: PostgresDatatype): PostgresColumnDefinition {
            return PostgresColumnDefinition("b_${datatype.name}", datatype)
        }

        /**
         * Utility function for retrieving the column definition for the data table.
         * @param indexType The index type for the column
         * @param edmType The [EdmPrimitiveTypeKind] of the column.
         *
         * @return The postgres column definition for the column time.
         */
        @JvmStatic
        fun getColumnDefinition(indexType: IndexType, edmType: EdmPrimitiveTypeKind): PostgresColumnDefinition {
            return columnDefinitionCache[indexType to edmType]
        }
    }

}