package com.openlattice.datasets

import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.organization.ExternalColumn
import com.openlattice.organization.ExternalTable
import java.util.*

data class DatasetColumn(
        val id: UUID,
        val datasetId: UUID,
        val name: String,
        val organizationId: UUID,
        val datatype: String,
        val metadata: SecurableObjectMetadata
) {

    companion object {
        @JvmStatic
        fun fromPropertyType(entitySet: EntitySet, propertyType: PropertyType, metadata: SecurableObjectMetadata): DatasetColumn {
            return DatasetColumn(
                    propertyType.id,
                    entitySet.id,
                    propertyType.type.toString(),
                    entitySet.organizationId,
                    propertyType.datatype.toString(),
                    metadata
            )
        }

        @JvmStatic
        fun fromExternalColumn(externalTable: ExternalTable, externalColumn: ExternalColumn, metadata: SecurableObjectMetadata): DatasetColumn {
            return DatasetColumn(
                    externalColumn.id,
                    externalTable.id,
                    externalColumn.name,
                    externalColumn.organizationId,
                    externalColumn.dataType.toString(),
                    metadata
            )
        }
    }

}