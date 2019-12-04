package com.openlattice.assembler.processors

import com.hazelcast.core.Offloadable
import com.hazelcast.spi.ExecutionService
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import java.util.*

data class EntitySetContainsFlagEntryProcessor(val flag: EntitySetFlag)
    : AbstractReadOnlyRhizomeEntryProcessor<UUID, EntitySet, Boolean>(), Offloadable {

    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet>): Boolean {
        return entry.value.flags.contains(flag)
    }

    override fun getExecutorName(): String {
        return ExecutionService.OFFLOADABLE_EXECUTOR
    }
}