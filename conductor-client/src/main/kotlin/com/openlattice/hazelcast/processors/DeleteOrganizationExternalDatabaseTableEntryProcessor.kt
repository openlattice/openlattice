package com.openlattice.hazelcast.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.organization.OrganizationExternalDatabaseTable
import java.util.*

class DeleteOrganizationExternalDatabaseTableEntryProcessor()
    : AbstractRhizomeEntryProcessor<UUID, OrganizationExternalDatabaseTable, Set<OrganizationExternalDatabaseTable>>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, OrganizationExternalDatabaseTable?>): Set<OrganizationExternalDatabaseTable>? {
        entry.setValue(null)
        return null
    }

}