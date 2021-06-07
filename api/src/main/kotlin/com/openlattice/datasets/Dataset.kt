package com.openlattice.datasets

import com.openlattice.edm.EntitySet
import com.openlattice.organization.ExternalTable
import java.util.*

data class Dataset(
        val id: UUID? = UUID.randomUUID(),
        val name: String,
        val organizationId: UUID,
        val externalId: String,
        val datasetType: DatasetType,
        val metadata: SecurableObjectMetadata
) {

    companion object {
        @JvmStatic
        fun fromEntitySet(entitySet: EntitySet, metadata: SecurableObjectMetadata): Dataset {
            return Dataset(
                    entitySet.id,
                    entitySet.name,
                    entitySet.organizationId,
                    entitySet.id.toString(),
                    DatasetType.EntitySet,
                    metadata
            )
        }

        @JvmStatic
        fun fromExternalTable(externalTable: ExternalTable, metadata: SecurableObjectMetadata): Dataset {
            return Dataset(
                    externalTable.id,
                    externalTable.name,
                    externalTable.organizationId,
                    externalTable.oid.toString(),
                    DatasetType.ExternalTable,
                    metadata
            )
        }
    }

    enum class DatasetType {
        EntitySet,
        ExternalTable
    }
}