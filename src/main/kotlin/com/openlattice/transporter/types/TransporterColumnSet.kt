package com.openlattice.transporter.types

import com.openlattice.ApiUtil
import com.openlattice.edm.PostgresEdmTypeConverter
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresDataTables
import com.openlattice.postgres.PostgresDatatype
import java.util.*

data class TransporterColumn(val srcCol: String, val destColName: String, val dataType: PostgresDatatype) {
    constructor(propertyType: PropertyType) :
        this(
                PostgresDataTables.getSourceDataColumnName(propertyType),
                ApiUtil.dbQuote(propertyType.id.toString()),
                PostgresEdmTypeConverter.map(propertyType.datatype)
        )
    fun destCol() = PostgresColumnDefinition(this.destColName, this.dataType)
}

data class TransporterColumnSet(val columns: Map<UUID, TransporterColumn>): Map<UUID, TransporterColumn> by columns {
    fun withProperties(properties: Collection<PropertyType>): TransporterColumnSet {
        val copy = columns.toMutableMap()
        properties.forEach {propertyType ->
            copy[propertyType.id] = TransporterColumn(propertyType)
        }
        return TransporterColumnSet(copy)
    }
}
