package com.openlattice.edm.processors

import com.hazelcast.core.Offloadable
import com.openlattice.organization.ExternalTable
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import java.util.*

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
class GetSchemaFromExternalTableEntryProcessor :
        AbstractReadOnlyRhizomeEntryProcessor<UUID, ExternalTable, String>(),
        Offloadable {
    override fun process(entry: MutableMap.MutableEntry<UUID, ExternalTable>): String {
        return entry.value.schema
    }

    override fun getExecutorName(): String {
        return Offloadable.OFFLOADABLE_EXECUTOR
    }
}