package com.openlattice.edm.processors

import com.openlattice.edm.EntitySet
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import java.util.*

class GetPartitionsFromEntitySetEntryProcessor : AbstractReadOnlyRhizomeEntryProcessor<UUID, EntitySet, Set<Int>>() {
    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet>): Set<Int> {
        return entry.value.partitions
    }
}