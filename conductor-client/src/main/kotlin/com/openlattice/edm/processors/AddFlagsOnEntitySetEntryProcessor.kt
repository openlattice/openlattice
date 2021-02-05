package com.openlattice.edm.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import java.util.*

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
class AddFlagsOnEntitySetEntryProcessor(val flags: EnumSet<EntitySetFlag>) : AbstractRhizomeEntryProcessor<UUID, EntitySet, Unit>() {
    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet>) {
        val es = entry.value
        es.flags.addAll(flags)
        entry.setValue(es)
    }
}