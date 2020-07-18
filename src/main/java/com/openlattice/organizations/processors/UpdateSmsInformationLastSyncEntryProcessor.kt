package com.openlattice.organizations.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.notifications.sms.SmsEntitySetInformation
import com.openlattice.notifications.sms.SmsInformationKey
import java.time.OffsetDateTime

class UpdateSmsInformationLastSyncEntryProcessor(
        val lastSync: OffsetDateTime
) : AbstractRhizomeEntryProcessor<SmsInformationKey, SmsEntitySetInformation, Unit>() {
    override fun process(entry: MutableMap.MutableEntry<SmsInformationKey, SmsEntitySetInformation>): Unit {
        val value = entry.value

        if (value == null) {
            return
        }

        value.lastSync = lastSync
        entry.setValue(value)
    }
}