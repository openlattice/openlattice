package com.openlattice.hazelcast.processors.organizations

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.organization.ExternalTable
import java.util.*

data class UpdateExternalTableEntryProcessor(val update: MetadataUpdate) :
        AbstractRhizomeEntryProcessor<UUID, ExternalTable, ExternalTable>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, ExternalTable>): ExternalTable {
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