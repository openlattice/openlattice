package com.openlattice.edm.processors

import com.hazelcast.core.ReadOnly
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.EntitySet
import java.util.*

class GetEntityTypeFromEntitySetEntryProcessor : AbstractRhizomeEntryProcessor<UUID, EntitySet, Optional<UUID>>(), ReadOnly {
    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet>?): Optional<UUID> {
        val es = entry?.value ?: return Optional.empty()
        return Optional.of( es.entityTypeId )
    }
}
