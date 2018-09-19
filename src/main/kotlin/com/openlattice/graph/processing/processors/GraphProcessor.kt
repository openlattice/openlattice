package com.openlattice.graph.processing.processors

import java.time.OffsetDateTime
import java.util.*

interface GraphProcessor {
    fun handledEntityTypes() : Set<UUID>
    /**
     * TODO
     */
    fun process( entities: Map<UUID, Map<UUID, Map<UUID, Set<Any>>>>, propagationStarted: OffsetDateTime)
    // entity set id -> entity key id ->
}