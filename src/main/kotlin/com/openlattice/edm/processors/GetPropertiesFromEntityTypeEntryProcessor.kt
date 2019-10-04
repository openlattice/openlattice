package com.openlattice.edm.processors

import com.openlattice.edm.type.EntityType
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import java.util.*

class GetPropertiesFromEntityTypeEntryProcessor: AbstractReadOnlyRhizomeEntryProcessor<UUID, EntityType, Set<UUID>>() {
    override fun process(entry: MutableMap.MutableEntry<UUID, EntityType?>): Set<UUID>? {
        val et = entry.value ?: return null
        return et.properties
    }
}
