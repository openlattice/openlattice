package com.openlattice.notifications.sms

import java.util.*

data class SubscriptionNotification(
        val messageContents: String,
        val phoneNumber: String
)