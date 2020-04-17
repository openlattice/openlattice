package com.openlattice.edm.processors

import com.openlattice.edm.EntitySet
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import java.util.*

class GetNormalEntitySetIdsEntryProcessor : AbstractReadOnlyRhizomeEntryProcessor<UUID, EntitySet, Set<UUID>?>() {
    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet?>): Set<UUID>? {
        val es = entry.value ?: return null
        return if (es.isLinking) es.linkedEntitySets else mutableSetOf(es.id)
    }
}