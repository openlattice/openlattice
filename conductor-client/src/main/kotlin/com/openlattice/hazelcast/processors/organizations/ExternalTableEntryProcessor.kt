package com.openlattice.hazelcast.processors.organizations

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.organization.ExternalTable
import org.slf4j.LoggerFactory
import java.util.*

private val logger = LoggerFactory.getLogger(ExternalTableEntryProcessor::class.java)

class ExternalTableEntryProcessor(
        val update: (ExternalTable) -> Result
) : AbstractRhizomeEntryProcessor<UUID, ExternalTable, Any?>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, ExternalTable?>): Any? {
        val table = entry.value
        if (table != null) {
            val (value, modified) = update(table)
            if (modified) {
                entry.setValue(table)
            }
            return value
        } else {
            logger.warn("Table not found when trying to update value.")
        }
        return null
    }

    data class Result(val value: Any? = null, val modified: Boolean = true)
}
