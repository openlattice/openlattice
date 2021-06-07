package com.openlattice.transporter.types

import com.openlattice.ApiHelpers
import com.openlattice.edm.PostgresEdmTypeConverter
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresDataTables
import com.openlattice.postgres.PostgresDatatype
import java.util.UUID

data class TransporterColumn(
        val dataTableColumnName: String,
        val transporterTableColumnName: String,
        val dataType: PostgresDatatype
) {
    constructor(propertyType: PropertyType) :
        this(
                PostgresDataTables.getSourceDataColumnName(propertyType),
                ApiHelpers.dbQuote(propertyType.id.toString()),
                PostgresEdmTypeConverter.map(propertyType.datatype)
        )
    fun transporterColumn() = PostgresColumnDefinition(this.transporterTableColumnName, this.dataType)
}

data class TransporterColumnSet(
        val columns: Map<UUID, TransporterColumn> // mapping of ptid to column info
): Map<UUID, TransporterColumn> by columns {
    fun withAndWithoutProperties(with: Collection<PropertyType>, without: Collection<PropertyType>): TransporterColumnSet {
        val copy = columns.toMutableMap()
        with.forEach { propertyType ->
            copy[propertyType.id] = TransporterColumn(propertyType)
        }
        without.forEach{ propertyType ->
            copy.remove(propertyType.id)
        }
        return TransporterColumnSet(copy)
    }
    fun withoutProperties(properties: Collection<PropertyType>): TransporterColumnSet {
        val copy = columns.toMutableMap()
        properties.forEach{ propertyType ->
            copy.remove(propertyType.id)
        }
        return TransporterColumnSet(copy)
    }

    fun withProperties(properties: Collection<PropertyType>): TransporterColumnSet {
        val copy = columns.toMutableMap()
        properties.forEach { propertyType ->
            copy[propertyType.id] = TransporterColumn(propertyType)
        }
        return TransporterColumnSet(copy)
    }
}
