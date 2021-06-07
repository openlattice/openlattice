package com.openlattice.datasets

import com.openlattice.authorization.AclKey
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.organization.ExternalColumn
import com.openlattice.organization.ExternalTable
import java.util.*

data class DataSetColumn(
    val id: UUID,
    val dataSetId: UUID,
    val name: String,
    val organizationId: UUID,
    val dataType: String,
    val metadata: SecurableObjectMetadata
) {

    fun getAclKey(): AclKey {
        return AclKey(dataSetId, id)
    }

    companion object {
        @JvmStatic
        fun fromPropertyType(
            entitySet: EntitySet,
            propertyType: PropertyType,
            metadata: SecurableObjectMetadata
        ): DataSetColumn {
            return DataSetColumn(
                propertyType.id,
                entitySet.id,
                propertyType.type.toString(),
                entitySet.organizationId,
                propertyType.datatype.toString(),
                metadata
            )
        }

        @JvmStatic
        fun fromExternalColumn(
            externalTable: ExternalTable,
            externalColumn: ExternalColumn,
            metadata: SecurableObjectMetadata
        ): DataSetColumn {
            return DataSetColumn(
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
