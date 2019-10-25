package com.openlattice.hazelcast.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRemover
import com.openlattice.organization.OrganizationExternalDatabaseTable
import java.util.*

class RemoveOrganizationExternalDatabaseTableEntryProcessor(val tables: Set<OrganizationExternalDatabaseTable>)
    : AbstractRemover<UUID, Set<OrganizationExternalDatabaseTable>, OrganizationExternalDatabaseTable>(tables) {
}