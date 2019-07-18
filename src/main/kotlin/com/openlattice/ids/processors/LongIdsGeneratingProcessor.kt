package com.openlattice.ids.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor

/**
 * Used to increment base ids.
 */
class LongIdsGeneratingProcessor(val count: Long) : AbstractRhizomeEntryProcessor<String, Long, Long>() {

    override fun process(entry: MutableMap.MutableEntry<String, Long?>): Long {
        val base = entry.value ?: Long.MIN_VALUE
        entry.setValue(base+count)
        return base
    }
}