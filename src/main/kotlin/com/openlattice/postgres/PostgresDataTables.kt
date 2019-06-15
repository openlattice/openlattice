package com.openlattice.postgres

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

        val dataTableColumns = listOf(
                ENTITY_SET_ID,
                ID_VALUE,
                PARTITION,
                PROPERTY_TYPE_ID,
                HASH,
                LAST_WRITE,
                LAST_MIGRATE,
                VERSION,
                VERSIONS
        ) + btreeIndexedColumns + ginIndexedColumns + nonIndexedColumns

        @JvmStatic
        fun buildDataTableDefinition(): PostgresTableDefinition {
            val columns = (dataTableColumns + listOf(
                    OWNERS,
                    READERS,
                    WRITERS
            )).toTypedArray()

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
    }

}