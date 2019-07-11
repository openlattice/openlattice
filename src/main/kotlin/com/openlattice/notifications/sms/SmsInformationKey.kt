package com.openlattice.notifications.sms

import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class SmsInformationKey(
        val phoneNumber: String,
        val organizationId: UUID
)