package com.openlattice.organizations.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.organization.OrganizationExternalDatabaseTable
import java.util.*

class UpdateOrganizationExternalDatabaseTable(val update: MetadataUpdate) :
        AbstractRhizomeEntryProcessor<UUID, OrganizationExternalDatabaseTable, OrganizationExternalDatabaseTable?>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, OrganizationExternalDatabaseTable>): OrganizationExternalDatabaseTable? {
        val table = entry.value

        if (update.title.isPresent) {
            table.title = update.title.get()
        }

        if (update.description.isPresent) {
            table.description = update.description.get()
        }

        if (update.organizationId.isPresent) {
            table.organizationId = update.organizationId.get()
        }

        entry.setValue(table)
        return table
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as UpdateOrganizationExternalDatabaseTable

        if (update != other.update) return false

        return true
    }

    override fun hashCode(): Int {
        return update.hashCode()
    }

    override fun toString(): String {
        return "UpdateOrganizationExternalDatabaseTable(update=$update)"
    }
}