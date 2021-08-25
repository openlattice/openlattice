package com.openlattice.datasets

import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.organization.ExternalColumn
import com.openlattice.organization.ExternalTable

data class SecurableObjectMetadata(
        var title: String,
        var description: String,
        var contacts: MutableSet<String> = mutableSetOf(),
        var flags: MutableSet<String> = mutableSetOf(),
        var metadata: MutableMap<String, Any> = mutableMapOf()
) {

    companion object {
        fun fromEntitySet(entitySet: EntitySet): SecurableObjectMetadata {
            return SecurableObjectMetadata(
                    entitySet.title,
                    entitySet.description,
                    entitySet.contacts,
                    entitySet.flags.mapTo(mutableSetOf()) { it.toString() }
            )
        }

        fun fromExternalTable(externalTable: ExternalTable): SecurableObjectMetadata {
            return SecurableObjectMetadata(
                    externalTable.title,
                    externalTable.description
            )
        }

        fun fromPropertyType(propertyType: PropertyType, flags: MutableSet<String> = mutableSetOf()): SecurableObjectMetadata {
            return SecurableObjectMetadata(
                    propertyType.title,
                    propertyType.description,
                    flags = flags
            )
        }

        fun fromExternalColumn(externalColumn: ExternalColumn): SecurableObjectMetadata {
            return SecurableObjectMetadata(
                    externalColumn.title,
                    externalColumn.description
            )
        }
    }
}