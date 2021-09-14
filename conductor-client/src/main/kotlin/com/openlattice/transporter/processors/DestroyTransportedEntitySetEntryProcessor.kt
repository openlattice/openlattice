package com.openlattice.transporter.processors

import com.hazelcast.core.Offloadable
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.transporter.types.TransporterDatastore
import com.openlattice.transporter.types.TransporterDependent
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import java.util.*

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@SuppressFBWarnings(value = ["RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE"], justification = "Ignore internal kotlin redundant nullchecks")
class DestroyTransportedEntitySetEntryProcessor(): AbstractRhizomeEntryProcessor<UUID, EntitySet, Void?>(),
        Offloadable,
        TransporterDependent<DestroyTransportedEntitySetEntryProcessor>
{

    @Transient
    private lateinit var data: TransporterDatastore

    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet>): Void? {
        check(::data.isInitialized) { TransporterDependent.NOT_INITIALIZED }
        val es = entry.value

        data.destroyEntitySetViewInOrgDb( es.organizationId, es.name )

        data.destroyTransportedEntityTypeTableInOrg( es.organizationId, es.entityTypeId )

        es.flags.remove(EntitySetFlag.TRANSPORTED)
        entry.setValue( es )
        return null
    }

    override fun getExecutorName(): String {
        return Offloadable.OFFLOADABLE_EXECUTOR
    }

    override fun init(data: TransporterDatastore): DestroyTransportedEntitySetEntryProcessor {
        this.data = data
        return this
    }
}