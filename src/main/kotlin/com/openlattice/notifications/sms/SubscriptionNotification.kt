package com.openlattice.notifications.sms

data class SubscriptionNotification(
        val messageContents: String,
        val phoneNumber: String
)