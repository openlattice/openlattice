package com.openlattice.transporter.processors

import com.hazelcast.core.Offloadable
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.EntitySet
import com.openlattice.transporter.destroyEntitySetView
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
        data.createOrgDataSource( entry.key ).connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate( destroyEntitySetView( entry.value.name ) )
            }
        }
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