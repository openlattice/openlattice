package com.openlattice.organizations.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import java.util.*

class UpdateOrganizationExternalDatabaseColumn(val update: MetadataUpdate, val maybeNewTableId: Optional<UUID>) :
        AbstractRhizomeEntryProcessor<UUID, OrganizationExternalDatabaseColumn, OrganizationExternalDatabaseColumn?>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, OrganizationExternalDatabaseColumn>): OrganizationExternalDatabaseColumn? {
        val column = entry.value

        if (update.title.isPresent) {
            column.title = update.title.get()
        }

        if (update.description.isPresent) {
            column.description = update.description.get()
        }

        if (update.organizationId.isPresent) {
            column.organizationId = update.organizationId.get()
        }

        if (maybeNewTableId.isPresent) {
            column.tableId = maybeNewTableId.get()
        }

        entry.setValue(column)
        return column
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as UpdateOrganizationExternalDatabaseColumn

        if (update != other.update) return false

        return true
    }

    override fun hashCode(): Int {
        return update.hashCode()
    }

    override fun toString(): String {
        return "UpdateOrganizationExternalDatabaseColumn(update=$update)"
    }

}