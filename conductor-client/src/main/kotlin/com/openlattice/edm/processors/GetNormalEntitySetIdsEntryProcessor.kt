package com.openlattice.edm.processors

import com.openlattice.edm.EntitySet
import com.geekbeast.hazelcast.DelegatedUUIDSet
import com.geekbeast.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import java.util.*

class GetNormalEntitySetIdsEntryProcessor : AbstractReadOnlyRhizomeEntryProcessor<UUID, EntitySet, DelegatedUUIDSet?>() {
    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet?>): DelegatedUUIDSet? {
        val es = entry.value ?: return null
        return if (es.isLinking) DelegatedUUIDSet(es.linkedEntitySets) else DelegatedUUIDSet(
            setOf(es.id)
        )
    }
}