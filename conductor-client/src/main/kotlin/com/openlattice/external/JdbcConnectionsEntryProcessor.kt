package com.openlattice.external

import com.hazelcast.core.Offloadable
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.hazelcast.processors.EpResult
import org.slf4j.LoggerFactory
import java.util.*

private val logger = LoggerFactory.getLogger(JdbcConnectionsEntryProcessor::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

class JdbcConnectionsEntryProcessor(
        @Transient
        val jdbcConnections: JdbcConnections,
        val update: (JdbcConnections,JdbcConnections) -> EpResult
) : AbstractRhizomeEntryProcessor<UUID, JdbcConnections, Any?>(), Offloadable {

    override fun process(entry: MutableMap.MutableEntry<UUID, JdbcConnections?>): Any? {
        val currentConnections = entry.value
        if (currentConnections != null) {
            val (value, modified) = update(currentConnections, jdbcConnections)
            if (modified) {
                entry.setValue(currentConnections)
            }
            return value
        } else {
            logger.warn("Organization not found when trying to update value.")
        }
        return null
    }

    override fun getExecutorName(): String = Offloadable.OFFLOADABLE_EXECUTOR
}





