package com.openlattice.transporter.processors

import com.hazelcast.core.Offloadable
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.transporter.types.TransporterDatastore
import com.openlattice.transporter.types.TransporterDependent
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*

/**
 *
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
data class TransportEntitySetEntryProcessor(
        val ptIdToFqnColumns: Map<UUID, FullQualifiedName>,
        val organizationId: UUID,
        val usersToColumnPermissions: Map<String, List<String>>
): AbstractRhizomeEntryProcessor<UUID, EntitySet, Void?>(),
        Offloadable,
        TransporterDependent<TransportEntitySetEntryProcessor>
{
    @Transient
    private lateinit var data: TransporterDatastore

    companion object {
        private val logger = LoggerFactory.getLogger(TransportEntitySetEntryProcessor::class.java)
    }

    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet>): Void? {
        check(::data.isInitialized) { TransporterDependent.NOT_INITIALIZED }
        val es = entry.value
        val esName = es.name
        if ( es.flags.contains(EntitySetFlag.TRANSPORTED)){
            return null
        }
        es.flags.add(EntitySetFlag.TRANSPORTED)
        entry.setValue(es)

        try {
            data.linkOrgDbToTransporterDb( organizationId )

            data.destroyTransportedEntitySetFromOrg( organizationId, esName )

            data.destroyEntitySetViewFromTransporter( esName )

            data.createTransporterEntitySetView( esName, es.id, es.entityTypeId, ptIdToFqnColumns )

            data.createTransportedEntitySetInOrg( organizationId, esName, usersToColumnPermissions )
        } catch ( ex: Exception ) {
            logger.error("Marking entity set id as not materialized {}", entry.key, ex)
            es.flags.remove(EntitySetFlag.TRANSPORTED)
            entry.setValue( es )
        }
        return null
    }

    override fun getExecutorName(): String {
        return Offloadable.OFFLOADABLE_EXECUTOR
    }

    override fun init(data: TransporterDatastore): TransportEntitySetEntryProcessor {
        this.data = data
        return this
    }
}