package com.openlattice.data.ids

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.data.EntityKey

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class IdRefCountIncrementer : AbstractRhizomeEntryProcessor<EntityKey, Long, Void?>() {
    override fun process(entry: MutableMap.MutableEntry<EntityKey, Long?>): Void? {
        entry.setValue((entry.value ?: 0).inc())
        return null
    }
}