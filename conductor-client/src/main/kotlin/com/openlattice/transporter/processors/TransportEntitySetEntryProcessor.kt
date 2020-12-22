package com.openlattice.transporter.processors

import com.hazelcast.core.Offloadable
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.EntitySet
import com.openlattice.edm.PropertyTypeIdFqn
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.transporter.types.TransporterDatastore
import com.openlattice.transporter.types.TransporterDependent
import com.zaxxer.hikari.HikariDataSource
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.slf4j.LoggerFactory
import java.util.UUID

/**
 *
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@SuppressFBWarnings(value = ["RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE"], justification = "Ignore internal kotlin redundant nullchecks")
data class TransportEntitySetEntryProcessor(
        val organizationId: UUID,
        val orgHds: HikariDataSource,
        val ptIdToFqnColumns: Set<PropertyTypeIdFqn>,
        val usersToColumnPermissions: Map<String, List<String>>
) : AbstractRhizomeEntryProcessor<UUID, EntitySet, Void?>(),
        Offloadable,
        TransporterDependent<TransportEntitySetEntryProcessor> {
    @Transient
    private lateinit var data: TransporterDatastore

    companion object {
        private val logger = LoggerFactory.getLogger(TransportEntitySetEntryProcessor::class.java)
    }

    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet>): Void? {
        check(::data.isInitialized) { TransporterDependent.NOT_INITIALIZED }
        val es = entry.value
        if (es.flags.contains(EntitySetFlag.TRANSPORTED)) {
            return null
        }

        try {
            data.transportEntitySet(
                    organizationId,
                    orgHds,
                    es,
                    ptIdToFqnColumns,
                    usersToColumnPermissions
            )
            es.flags.add(EntitySetFlag.TRANSPORTED)
        } catch (ex: Exception) {
            logger.error("Marking entity set id as not materialized {}", entry.key, ex)
            es.flags.remove(EntitySetFlag.TRANSPORTED)
        } finally {
            entry.setValue(es)
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