package com.openlattice.transporter.processors

import com.hazelcast.core.Offloadable
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.transporter.types.TransporterDatastore
import com.openlattice.transporter.types.TransporterDependent
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import java.util.UUID

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@SuppressFBWarnings(value = ["RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE"], justification = "Ignore internal kotlin redundant nullchecks")
class MarkEntitySetNotTransportedEntryProcessor : AbstractRhizomeEntryProcessor<UUID, EntitySet, Void?>(), Offloadable {

    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet>): Void? {
        val es = entry.value
        es.flags.remove(EntitySetFlag.TRANSPORTED)
        entry.setValue(es)
        return null
    }

    override fun getExecutorName(): String {
        return Offloadable.OFFLOADABLE_EXECUTOR
    }
}