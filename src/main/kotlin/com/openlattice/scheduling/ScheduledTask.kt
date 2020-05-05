package com.openlattice.scheduling

import java.time.OffsetDateTime
import java.util.*

data class ScheduledTask(
        val id: UUID = UUID.randomUUID(),
        val scheduledDateTime: OffsetDateTime,
        val task: RunnableTask
)