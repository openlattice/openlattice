package com.openlattice.scheduling

import java.time.OffsetDateTime
import java.util.*

data class ScheduledTask(
        val id: UUID = UUID.randomUUID(),
        val scheduledDateTime: OffsetDateTime,
        val task: RunnableTask

) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ScheduledTask) return false

        if (id != other.id) return false
        if (scheduledDateTime.toInstant() != other.scheduledDateTime.toInstant()) return false
        if (task != other.task) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + scheduledDateTime.hashCode()
        result = 31 * result + task.hashCode()
        return result
    }
}