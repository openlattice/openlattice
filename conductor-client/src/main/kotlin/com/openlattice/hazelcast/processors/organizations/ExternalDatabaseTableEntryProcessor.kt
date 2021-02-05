package com.openlattice.hazelcast.processors.organizations

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.organization.OrganizationExternalDatabaseTable
import org.slf4j.LoggerFactory
import java.util.*

private val logger = LoggerFactory.getLogger(ExternalDatabaseTableEntryProcessor::class.java)

class ExternalDatabaseTableEntryProcessor(
        val update: (OrganizationExternalDatabaseTable) -> Result
) : AbstractRhizomeEntryProcessor<UUID, OrganizationExternalDatabaseTable, Any?>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, OrganizationExternalDatabaseTable?>): Any? {
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
