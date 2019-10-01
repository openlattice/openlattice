package com.openlattice.conductor.rpc

import java.io.Serializable
import java.util.*
import java.util.function.Function

data class BulkLinkedDataLambdas(
        val entityTypeId: UUID,
        val entitiesByLinkingId: Map<UUID, Map<UUID, Map<UUID, Map<UUID, Set<Object>>>>>
) : Function<ConductorElasticsearchApi, Boolean>, Serializable {

    override fun apply(conductorElasticsearchApi: ConductorElasticsearchApi): Boolean {
        return conductorElasticsearchApi.createBulkLinkedData(entityTypeId, entitiesByLinkingId)
    }
}
