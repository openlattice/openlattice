package com.openlattice.linking

import java.util.*

interface LinkingLogService {
    fun createOrUpdateCluster(linkingId: UUID, linkedEntities: Map<UUID, Set<UUID>>, newCluster: Boolean)

    fun clearEntitiesFromCluster(linkingId: UUID, linkedEntities: Map<UUID, Set<UUID>>)

    fun readLatestLinkLog(linkingId: UUID): Map<UUID, Set<UUID>>

    fun readVersion(linkingId: UUID, version: Long): Map<UUID, Set<UUID>>
}