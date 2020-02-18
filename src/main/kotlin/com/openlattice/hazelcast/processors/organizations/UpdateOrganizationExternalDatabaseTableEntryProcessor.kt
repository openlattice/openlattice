package com.openlattice.hazelcast.processors.organizations

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.organization.OrganizationExternalDatabaseTable
import java.util.*

data class UpdateOrganizationExternalDatabaseTableEntryProcessor(val update: MetadataUpdate):
        AbstractRhizomeEntryProcessor<UUID, OrganizationExternalDatabaseTable, OrganizationExternalDatabaseTable>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, OrganizationExternalDatabaseTable>): OrganizationExternalDatabaseTable {
        val table = entry.value

        update.title.ifPresent {
            table.title = it
        }

        update.name.ifPresent {
            table.name = it
        }

        update.description.ifPresent {
            table.description = it
        }

        update.organizationId.ifPresent {
            table.organizationId = it
        }

        entry.setValue(table)
        return table
    }

}