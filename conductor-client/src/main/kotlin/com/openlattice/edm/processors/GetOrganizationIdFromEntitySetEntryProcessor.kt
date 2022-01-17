package com.openlattice.edm.processors

import com.hazelcast.core.Offloadable
import com.openlattice.edm.EntitySet
import com.geekbeast.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import java.util.*

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
class GetOrganizationIdFromEntitySetEntryProcessor:
    AbstractReadOnlyRhizomeEntryProcessor<UUID, EntitySet, UUID>(),
    Offloadable
{
    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet>): UUID {
        return entry.value.organizationId
    }

    override fun getExecutorName(): String {
        return Offloadable.OFFLOADABLE_EXECUTOR
    }
}