package com.openlattice.edm.processors

import com.openlattice.edm.type.EntityType
import com.geekbeast.hazelcast.DelegatedUUIDSet
import com.geekbeast.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import java.util.*

class GetPropertiesFromEntityTypeEntryProcessor: AbstractReadOnlyRhizomeEntryProcessor<UUID, EntityType, DelegatedUUIDSet>() {
    override fun process(entry: MutableMap.MutableEntry<UUID, EntityType?>): DelegatedUUIDSet? {
        val et = entry.value ?: return null
        return DelegatedUUIDSet(et.properties)
    }
}
