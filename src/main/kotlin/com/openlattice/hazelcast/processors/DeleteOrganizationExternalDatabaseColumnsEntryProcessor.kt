package com.openlattice.hazelcast.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import java.util.*

class DeleteOrganizationExternalDatabaseColumnsEntryProcessor()
    : AbstractRhizomeEntryProcessor<UUID, Set<OrganizationExternalDatabaseColumn>, Set<OrganizationExternalDatabaseColumn>>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, Set<OrganizationExternalDatabaseColumn>?>): Set<OrganizationExternalDatabaseColumn>? {
        entry.setValue(null)
        return null
    }

}