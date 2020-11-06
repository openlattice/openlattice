package com.openlattice.data

import java.util.*

data class CreateAssociationEvent(
        val ids: List<UUID>,
        val entityWriteEvent: WriteEvent,
        val edgeWriteEvent: WriteEvent
)