package com.openlattice.organizations.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.organizations.Organization
import org.slf4j.LoggerFactory
import java.util.*

private val logger = LoggerFactory.getLogger(OrganizationEntryProcessor::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

class OrganizationEntryProcessor(
        val update: (Organization) -> Result
) : AbstractRhizomeEntryProcessor<UUID, Organization, Any?>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, Organization?>): Any? {
        val organization = entry.value
        if (organization != null) {
            val (value, modified) = update(organization)
            if (modified) {
                entry.setValue(organization)
            }
            return value
        } else {
            logger.warn("Organization not found when trying to update value.")
        }
        return null
    }
    data class Result( val value: Any?, val modified: Boolean = true )
}





