package com.openlattice.codex

import java.time.OffsetDateTime
import java.util.UUID

data class MessageRequest (
        val organizationId: UUID,
        val messageEntitySetId : UUID,
        val messageContents: String,
        val phoneNumber: String,
        var senderId: String = "",
        val attachment: Base64Media? = null,
        val scheduledDateTime: OffsetDateTime = OffsetDateTime.now()
)
