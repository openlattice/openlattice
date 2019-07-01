package com.openlattice.linking

import java.util.*

interface LinkingLogService {
    fun logEntitiesAddedToLink(linkingId: UUID, linkedEntities: Map<UUID, Set<UUID>>)

    fun logEntitiesRemovedFromLink(linkingId: UUID, linkedEntities: Map<UUID, Set<UUID>>)

    fun logLinkCreated(linkingId: UUID, linkedEntities: Map<UUID, Set<UUID>>)
    fun readLatestLinkLog(linkingId: UUID)
}