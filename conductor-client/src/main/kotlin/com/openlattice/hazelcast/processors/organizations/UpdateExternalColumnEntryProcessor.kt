package com.openlattice.hazelcast.processors.organizations

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.organization.ExternalColumn
import java.util.*

data class UpdateExternalColumnEntryProcessor(val update: MetadataUpdate) :
        AbstractRhizomeEntryProcessor<UUID, ExternalColumn, ExternalColumn>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, ExternalColumn>): ExternalColumn {
        val column = entry.value

        update.title.ifPresent {
            column.title = it
        }

        update.name.ifPresent {
            column.name = it
        }

        update.description.ifPresent {
            column.description = it
        }

        update.organizationId.ifPresent {
            column.organizationId = it
        }

        entry.setValue(column)
        return column
    }
}