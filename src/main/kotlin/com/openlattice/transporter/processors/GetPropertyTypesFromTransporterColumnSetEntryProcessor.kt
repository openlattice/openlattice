package com.openlattice.transporter.processors

import com.hazelcast.core.Offloadable
import com.openlattice.rhizome.KotlinDelegatedUUIDSet
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import com.openlattice.transporter.types.TransporterColumnSet
import java.util.UUID

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class GetPropertyTypesFromTransporterColumnSetEntryProcessor:
        AbstractReadOnlyRhizomeEntryProcessor<UUID, TransporterColumnSet, Set<UUID>>(),
        Offloadable
{
    override fun process(entry: MutableMap.MutableEntry<UUID, TransporterColumnSet>): Set<UUID> {
        return KotlinDelegatedUUIDSet( entry.value.keys )
    }

    override fun getExecutorName(): String {
        return Offloadable.OFFLOADABLE_EXECUTOR
    }
}