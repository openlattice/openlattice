package com.openlattice.graph.processing.processors

import com.openlattice.data.storage.PostgresEntityDataQueryService
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.time.OffsetDateTime
import java.util.UUID

class NoneProcessor(private val entityDataService: PostgresEntityDataQueryService): GraphProcessor {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getSql(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun process(entities: Map<UUID, Map<UUID, Map<UUID, Set<Any>>>>, propagationStarted: OffsetDateTime) {
        entities.forEach { entityDataService.markAsProcessed(it.key, it.value.keys, propagationStarted) }
    }
}