package com.openlattice.edm.processors

import com.openlattice.edm.EntitySet
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import java.util.*

class GetEntityTypeFromEntitySetEntryProcessor : AbstractReadOnlyRhizomeEntryProcessor<UUID, EntitySet, UUID?>() {
    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet?>): UUID? {
        val es = entry.value ?: return null
        return es.entityTypeId
    }
}
