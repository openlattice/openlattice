package com.openlattice.conductor.rpc

import java.io.Serializable
import java.util.UUID
import java.util.function.Function

data class BulkLinkedDataLambdas(
        val linkingEntitySetId: UUID,
        val entitiesByLinkingId: Map<UUID, Map<UUID, Map<UUID, Set<Any>>>>
): Function<ConductorElasticsearchApi, Boolean>, Serializable {

    override fun apply(  conductorElasticsearchApi: ConductorElasticsearchApi ): Boolean {
        return conductorElasticsearchApi.createBulkLinkedData( linkingEntitySetId, entitiesByLinkingId )
    }
}
