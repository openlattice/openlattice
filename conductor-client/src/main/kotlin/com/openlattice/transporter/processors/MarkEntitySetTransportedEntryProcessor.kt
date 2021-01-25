package com.openlattice.transporter.processors

import com.hazelcast.core.Offloadable
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.EntitySet
import com.openlattice.edm.PropertyTypeIdFqn
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.transporter.types.TransporterDatastore
import com.openlattice.transporter.types.TransporterDependent
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.slf4j.LoggerFactory
import java.util.UUID

/**
 *
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@SuppressFBWarnings(
        value = ["RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", "SE_BAD_FIELD"],
        justification = "Ignore internal kotlin redundant nullchecks, ignore bad field b/c custom serializers used"
)
class MarkEntitySetTransportedEntryProcessor : AbstractRhizomeEntryProcessor<UUID, EntitySet, Void?>(), Offloadable {

    companion object {
        private val logger = LoggerFactory.getLogger(MarkEntitySetTransportedEntryProcessor::class.java)
    }

    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet>): Void? {
        val es = entry.value
        es.flags.add(EntitySetFlag.TRANSPORTED)
        entry.setValue(es)
        return null
    }

    override fun getExecutorName(): String {
        return Offloadable.OFFLOADABLE_EXECUTOR
    }
}