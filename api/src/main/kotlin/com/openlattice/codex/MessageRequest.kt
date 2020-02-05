package com.openlattice.codex

import java.util.UUID

data class MessageRequest (
        val organizationId: UUID,
        val messageEntitySetId : UUID,
        val messageContents: String,
        val phoneNumber: String,
        var senderId: String? = null
)
