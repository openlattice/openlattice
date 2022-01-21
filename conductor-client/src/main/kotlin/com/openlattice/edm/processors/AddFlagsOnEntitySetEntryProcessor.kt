package com.openlattice.edm.processors

import com.geekbeast.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import java.util.*

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
class AddFlagsOnEntitySetEntryProcessor(val flags: EnumSet<EntitySetFlag>) : AbstractRhizomeEntryProcessor<UUID, EntitySet, Void>() {
    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet>): Void? {
        val es = entry.value
        es.flags.addAll(flags)
        entry.setValue(es)
        return null
    }
}