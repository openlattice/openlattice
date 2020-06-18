package com.openlattice.edm.processors

import com.openlattice.edm.EntitySet
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import java.util.*

class GetPartitionsFromEntitySetEntryProcessor : AbstractReadOnlyRhizomeEntryProcessor<UUID, EntitySet, Set<Int>>() {
    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet?>): Set<Int> {
        val value = entry.value
        if ( value == null ){
            return setOf()
        }
        return value.partitions
    }
}