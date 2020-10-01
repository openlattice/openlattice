package com.openlattice.transporter.processors

import com.hazelcast.core.Offloadable
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.transporter.destroyEntitySetView
import com.openlattice.transporter.dropOrgViewTable
import com.openlattice.transporter.types.TransporterDatastore
import com.openlattice.transporter.types.TransporterDependent
import java.util.*

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class DestroyTransportedEntitySetEntryProcessor: AbstractRhizomeEntryProcessor<UUID, EntitySet, Void?>(),
        Offloadable,
        TransporterDependent<DestroyTransportedEntitySetEntryProcessor> {

    @Transient
    private lateinit var data: TransporterDatastore

    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet>): Void? {
        val es = entry.value
        data.datastore().connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate( destroyEntitySetView( es.name ) )
            }
        }

        data.connectOrgDb( es.organizationId ).use { hds ->
            hds.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeUpdate( dropOrgViewTable( es.name ))
                }
            }
        }

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