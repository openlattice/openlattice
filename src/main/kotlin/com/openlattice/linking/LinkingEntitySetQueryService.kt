package com.openlattice.linking

import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.datastore.services.EdmManager
import java.util.UUID

class LinkingEntitySetQueryService(
        private val dataQueryService: PostgresEntityDataQueryService,
        private val edm: EdmManager ) {

    fun getEntitiesWithLinkingIds( linkingIds:Set<UUID> ): Map<UUID, Set<UUID>> {
        val linkedEntityIds = dataQueryService.getEntityDataKeysOfLinkingIds(linkingIds)

        return linkedEntityIds.groupBy { it.entitySetId }.mapValues { it.value.map { it.entityKeyId }.toSet() }
    }

}