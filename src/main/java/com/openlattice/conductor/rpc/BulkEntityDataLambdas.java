package com.openlattice.conductor.rpc;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

public class BulkEntityDataLambdas implements Function<ConductorElasticsearchApi, Boolean>, Serializable {

    private UUID                              entitySetId;
    private Map<UUID, Map<UUID, Set<Object>>> entitiesById;

    public BulkEntityDataLambdas(
            UUID entitySetId,
            Map<UUID, Map<UUID, Set<Object>>> entitiesById ) {
        this.entitySetId = entitySetId;
        this.entitiesById = entitiesById;
    }

    @Override
    public Boolean apply( ConductorElasticsearchApi conductorElasticsearchApi ) {
        return conductorElasticsearchApi.createBulkEntityData( entitySetId, entitiesById );
    }

    public UUID getEntitySetId() {
        return entitySetId;
    }

    public Map<UUID, Map<UUID, Set<Object>>> getEntitiesById() {
        return entitiesById;
    }
}
