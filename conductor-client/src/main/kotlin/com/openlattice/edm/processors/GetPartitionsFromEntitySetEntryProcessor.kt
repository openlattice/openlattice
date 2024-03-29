package com.openlattice.edm.processors

import com.openlattice.edm.EntitySet
import com.geekbeast.rhizome.DelegatedIntSet
import com.geekbeast.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import java.util.*

class GetPartitionsFromEntitySetEntryProcessor : AbstractReadOnlyRhizomeEntryProcessor<UUID, EntitySet, DelegatedIntSet>() {
    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet?>): DelegatedIntSet {
        val value = entry.value ?: return DelegatedIntSet(setOf())
        return DelegatedIntSet(value.partitions)
    }
}