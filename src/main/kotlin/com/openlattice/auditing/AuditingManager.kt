package com.openlattice.auditing

interface AuditingManager {

    fun recordEvents(events: List<AuditableEvent>): Int

}