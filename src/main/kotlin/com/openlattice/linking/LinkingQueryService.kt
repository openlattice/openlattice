package com.openlattice.linking

import com.openlattice.data.storage.PostgresEntityDataQueryService
import java.util.UUID

class LinkingQueryService(private val dataQueryService: PostgresEntityDataQueryService) {

    fun getEntitiesWithLinkingIds( linkingIds:Set<UUID> ): Map<UUID, Set<UUID>> {
        val linkedEntityIds = dataQueryService.getEntityKeysOfLinkingIds(linkingIds)

        return linkedEntityIds.groupBy { it.entitySetId }.mapValues { it.value.map { it.entityKeyId }.toSet() }
    }
}