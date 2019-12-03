package com.openlattice.assembler.processors

import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import java.util.*

data class EntitySetContainsFlagEntryProcessor(val flag: EntitySetFlag)
    : AbstractReadOnlyRhizomeEntryProcessor<UUID, EntitySet, Boolean>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet>): Boolean {
        return entry.value.flags.contains(flag)
    }
}