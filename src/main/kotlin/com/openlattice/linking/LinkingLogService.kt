package com.openlattice.linking

import java.util.*

interface LinkingLogService {
    fun updateCluster(linkingId: UUID, linkedEntities: Map<UUID, Set<UUID>>)

    fun removeEntitiesFromCluster(linkingId: UUID, linkedEntities: Map<UUID, Set<UUID>>)

    fun createCluster(linkingId: UUID, linkedEntities: Map<UUID, Set<UUID>>)

    fun readLatestLinkLog(linkingId: UUID): Map<UUID, Set<UUID>>

    fun readVersion(linkingId: UUID, version: Long): Map<UUID, Set<UUID>>
}