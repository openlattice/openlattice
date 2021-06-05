package com.openlattice.organizations.processors

import com.openlattice.organizations.Organization
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import org.slf4j.LoggerFactory
import java.util.*

private val logger = LoggerFactory.getLogger(OrganizationEntryProcessor::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class OrganizationReadEntryProcessor(val read : (organization:Organization) -> Any ) : AbstractReadOnlyRhizomeEntryProcessor<UUID, Organization, Any?>() {
    override fun process(entry: MutableMap.MutableEntry<UUID, Organization?>): Any? {
        val organization = entry.value
        return if( organization == null ) {
            null
        } else {
            read( organization )
        }
    }
}

