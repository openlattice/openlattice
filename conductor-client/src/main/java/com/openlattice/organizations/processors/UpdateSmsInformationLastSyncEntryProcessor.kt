package com.openlattice.organizations.processors

import com.geekbeast.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.notifications.sms.SmsEntitySetInformation
import com.openlattice.notifications.sms.SmsInformationKey
import java.time.OffsetDateTime

data class UpdateSmsInformationLastSyncEntryProcessor(
        val lastSync: OffsetDateTime
) : AbstractRhizomeEntryProcessor<SmsInformationKey, SmsEntitySetInformation, Boolean>() {
    override fun process(entry: MutableMap.MutableEntry<SmsInformationKey, SmsEntitySetInformation>): Boolean {
        val value = entry.value ?: return false

        value.lastSync = lastSync
        entry.setValue(value)
        return true
    }
}