package com.openlattice.edm.processors

import com.hazelcast.core.ReadOnly
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.type.EntityType
import java.util.*

class GetPropertiesFromEntityTypeEntryProcessor: AbstractRhizomeEntryProcessor<UUID, EntityType, Optional<Set<UUID>>>(), ReadOnly {
    override fun process(entry: MutableMap.MutableEntry<UUID, EntityType?>): Optional<Set<UUID>>? {
        val et = entry.value ?: return Optional.empty()
        return Optional.of( et.properties )
    }
}
