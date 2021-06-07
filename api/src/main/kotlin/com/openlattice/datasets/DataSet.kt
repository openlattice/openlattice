package com.openlattice.datasets

import com.openlattice.edm.EntitySet
import com.openlattice.organization.ExternalTable
import java.util.*

data class DataSet(
    val id: UUID? = UUID.randomUUID(),
    val name: String,
    val organizationId: UUID,
    val externalId: String,
    val datasetType: DataSetType,
    val metadata: SecurableObjectMetadata
) {

    companion object {
        @JvmStatic
        fun fromEntitySet(entitySet: EntitySet, metadata: SecurableObjectMetadata): DataSet {
            return DataSet(
                entitySet.id,
                entitySet.name,
                entitySet.organizationId,
                entitySet.id.toString(),
                DataSetType.EntitySet,
                metadata
            )
        }

        @JvmStatic
        fun fromExternalTable(externalTable: ExternalTable, metadata: SecurableObjectMetadata): DataSet {
            return DataSet(
                externalTable.id,
                externalTable.name,
                externalTable.organizationId,
                externalTable.oid.toString(),
                DataSetType.ExternalTable,
                metadata
            )
        }
    }

    enum class DataSetType {
        EntitySet,
        ExternalTable
    }
}
