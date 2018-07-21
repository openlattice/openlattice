package com.openlattice.conductor.rpc;

import com.google.common.collect.SetMultimap;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

public class BulkEntityDataLambdas implements Function<ConductorElasticsearchApi, Boolean>, Serializable {

    private UUID entitySetId;
    private Map<UUID, SetMultimap<UUID, Object>> entitiesById;

    public BulkEntityDataLambdas( UUID entitySetId, Map<UUID, SetMultimap<UUID, Object>> entitiesById ){
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

    public Map<UUID, SetMultimap<UUID, Object>> getEntitiesById() {
        return entitiesById;
    }
}
