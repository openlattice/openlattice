package com.openlattice.ids.processors

import com.hazelcast.core.Offloadable
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor

/**
 * Used to increment base ids.
 */
class LongIdsGeneratingProcessor(val count: Long) : Offloadable, AbstractRhizomeEntryProcessor<String, Long, Long>() {

    override fun process(entry: MutableMap.MutableEntry<String, Long?>): Long {
        val base = entry.value ?: 0
        entry.setValue(base+count)
        return base
    }

    override fun getExecutorName(): String = Offloadable.OFFLOADABLE_EXECUTOR
}