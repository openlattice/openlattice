package com.openlattice.graph.processing.processors

import com.openlattice.data.storage.PostgresEntityDataQueryService
import java.time.OffsetDateTime
import java.util.UUID

class NoneProcessor(private val entityDataService: PostgresEntityDataQueryService):
        GraphProcessor {
    override fun process(entities: Map<UUID, Map<UUID, Map<UUID, Set<Any>>>>, propagationStarted: OffsetDateTime) {
        entities.forEach { entityDataService.markAsProcessed(it.key, it.value.keys, propagationStarted) }
    }
}