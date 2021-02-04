package com.openlattice.external

import com.hazelcast.core.Offloadable
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import org.slf4j.LoggerFactory
import java.util.*

private val logger = LoggerFactory.getLogger(AddJdbcConnectionsEntryProcessor::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

class AddJdbcConnectionsEntryProcessor(
        val jdbcConnections: JdbcConnections
) : AbstractRhizomeEntryProcessor<UUID, JdbcConnections, Any?>(), Offloadable {

    override fun process(entry: MutableMap.MutableEntry<UUID, JdbcConnections?>): Any? {
        val currentConnections = entry.value
        if (currentConnections != null) {
            currentConnections.putAll(jdbcConnections)
            entry.setValue(currentConnections)
        } else {
            entry.setValue(jdbcConnections)
        }
        return null
    }

    override fun getExecutorName(): String = Offloadable.OFFLOADABLE_EXECUTOR
}
