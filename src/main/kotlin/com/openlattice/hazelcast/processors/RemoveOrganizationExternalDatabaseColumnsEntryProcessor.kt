package com.openlattice.hazelcast.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRemover
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import java.util.*

data class RemoveOrganizationExternalDatabaseColumnsEntryProcessor(val columns: Set<OrganizationExternalDatabaseColumn>)
    : AbstractRemover<UUID, Set<OrganizationExternalDatabaseColumn>, OrganizationExternalDatabaseColumn>(columns)