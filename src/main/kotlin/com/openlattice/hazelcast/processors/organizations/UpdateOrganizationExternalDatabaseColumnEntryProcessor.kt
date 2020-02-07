package com.openlattice.hazelcast.processors.organizations

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import java.util.*

data class UpdateOrganizationExternalDatabaseColumnEntryProcessor(val update: MetadataUpdate) :
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

        entry.setValue(column)
        return column
    }
}