package com.openlattice.organizations.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.notifications.sms.SmsEntitySetInformation
import com.openlattice.organizations.Organization
import org.slf4j.LoggerFactory
import java.util.*

private val logger = LoggerFactory.getLogger(UpdateOrganizationSmsEntitySetInformationEntryProcessor::class.java)

data class UpdateOrganizationSmsEntitySetInformationEntryProcessor(val smsEntitySetInformation: List<SmsEntitySetInformation>) : AbstractRhizomeEntryProcessor<UUID, Organization, Any?>() {
    override fun process(entry: MutableMap.MutableEntry<UUID, Organization>): Any? {
        val organization = entry.value
        if (organization != null) {
            organization.smsEntitySetInfo.clear()
            organization.smsEntitySetInfo.addAll(smsEntitySetInformation)
            entry.setValue(organization)
        } else {
            logger.warn("Organization not found when trying to update value.")
        }
        return null
    }

}