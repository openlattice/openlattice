package com.openlattice.linking

import java.util.*

interface LinkingLogService {
    fun createOrUpdateForLinks( linkingId: UUID, linkedEntities: Map<UUID, Set<UUID>> )

    fun clearLink(linkingId: UUID, linkedEntities: Map<UUID, Set<UUID>>)
}