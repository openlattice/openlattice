package com.openlattice.hazelcast.processors.organizations

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.organization.OrganizationExternalDatabaseTable
import java.util.*

data class UpdateOrganizationExternalDatabaseTableEntryProcessor(val update: MetadataUpdate):
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

}