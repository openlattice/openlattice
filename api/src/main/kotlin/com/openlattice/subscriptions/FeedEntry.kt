package com.openlattice.subscriptions

import com.openlattice.auditing.AuditEventType
import com.openlattice.authorization.Principal
import com.openlattice.data.EntityDataKey

data class FeedEntry (
        val edk: EntityDataKey,
        val eventType: AuditEventType,
        val involvedEdks: Set<EntityDataKey>,
        val eventPrincipal: Principal
) {

}
