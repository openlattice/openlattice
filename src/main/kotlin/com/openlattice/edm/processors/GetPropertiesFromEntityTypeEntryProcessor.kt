package com.openlattice.edm.processors

import com.hazelcast.core.ReadOnly
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.type.EntityType
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import java.util.*

class GetPropertiesFromEntityTypeEntryProcessor: AbstractReadOnlyRhizomeEntryProcessor<UUID, EntityType, Optional<Set<UUID>>>() {
    override fun process(entry: MutableMap.MutableEntry<UUID, EntityType?>): Optional<Set<UUID>>? {
        val et = entry.value ?: return Optional.empty()
        return Optional.of( et.properties )
    }
}
