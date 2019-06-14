package com.openlattice.postgres

import com.openlattice.edm.PostgresEdmTypeConverter
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresColumn.ID
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresDataTables {
    companion object {
        @JvmStatic
        fun buildEntityTypeTableDefinition(
                entityType: EntityType,
                propertyTypes: Collection<PropertyType>
        ): PostgresTableDefinition {
            check(propertyTypes.all { entityType.properties.contains(it.id) }) {
                "All property types must be provided for entity type."
            }

            val columns = arrayOf(ID) + propertyTypes.map(::value)

            return CitusDistributedTableDefinition(entityTypeTableName(entityType.id))
                    .addColumns(*columns)
                    .distributionColumn(ID)
        }

        @JvmStatic
        fun value(propertyType: PropertyType): PostgresColumnDefinition {
            return PostgresColumnDefinition(
                    quote(propertyType.type.fullQualifiedNameAsString),
                    getValueColumnPostgresDataType(propertyType)
            )
        }

        @JvmStatic
        fun entityTypeTableName(entityTypeId: UUID): String {
            return quote("et_$entityTypeId")
        }

        @JvmStatic
        fun getValueColumnPostgresDataType(propertyType: PropertyType): PostgresDatatype {
            return if (propertyType.isMultiValued) {
                PostgresEdmTypeConverter.mapToArrayType(propertyType.datatype)
            } else {
                PostgresEdmTypeConverter.map(propertyType.datatype)
            }
        }
    }

}