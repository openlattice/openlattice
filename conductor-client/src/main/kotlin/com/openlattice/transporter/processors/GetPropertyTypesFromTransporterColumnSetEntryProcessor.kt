package com.openlattice.transporter.processors

import com.hazelcast.core.Offloadable
import com.geekbeast.rhizome.KotlinDelegatedUUIDSet
import com.geekbeast.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import com.openlattice.transporter.types.TransporterColumnSet
import org.slf4j.LoggerFactory
import java.util.*

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class GetPropertyTypesFromTransporterColumnSetEntryProcessor:
    AbstractReadOnlyRhizomeEntryProcessor<UUID, TransporterColumnSet, Set<UUID>>(),
    Offloadable
{
    companion object {
        private val logger = LoggerFactory.getLogger(GetPropertyTypesFromTransporterColumnSetEntryProcessor::class.java)
    }

    override fun process(entry: MutableMap.MutableEntry<UUID, TransporterColumnSet?>): Set<UUID> {
        val cols = entry.value
        requireNotNull( cols ) {
            logger.error("No TransporterColumnSet found for entitytype ${entry.key}")
            "No TransporterColumnSet found for entitytype ${entry.key}"
        }
        return KotlinDelegatedUUIDSet( cols.keys )
    }

    override fun getExecutorName(): String {
        return Offloadable.OFFLOADABLE_EXECUTOR
    }
}