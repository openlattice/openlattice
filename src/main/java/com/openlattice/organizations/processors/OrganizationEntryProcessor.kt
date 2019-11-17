package com.openlattice.organizations.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.client.serialization.SerializableFunction
import com.openlattice.organizations.OepLambdas
import com.openlattice.organizations.Organization
import org.apache.commons.lang.RandomStringUtils
import org.slf4j.LoggerFactory
import java.io.Serializable
import java.util.*
import java.util.function.Consumer

private val logger = LoggerFactory.getLogger(OrganizationEntryProcessor::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

class OrganizationEntryProcessor(
        val update: (Organization) -> Unit
) : AbstractRhizomeEntryProcessor<UUID, Organization, Void?>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, Organization?>): Void? {
        val organization = entry.value
        if (organization != null) {
//            (update as (organization: Organization) -> Unit) (organization)
            update(organization)
            entry.setValue(organization)
        } else {
            logger.warn("Organization not found when trying to update value.")
        }
        return null
    }


}



