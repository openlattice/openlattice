package com.openlattice.edm.processors

import com.hazelcast.core.ReadOnly
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.EntitySet
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import java.util.*

class GetEntityTypeFromEntitySetEntryProcessor : AbstractReadOnlyRhizomeEntryProcessor<UUID, EntitySet, Optional<UUID>>() {
    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet?>): Optional<UUID> {
        val es = entry.value ?: return Optional.empty()
        return Optional.of( es.entityTypeId )
    }
}
