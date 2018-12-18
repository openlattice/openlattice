package com.openlattice.conductor.rpc;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

public class BulkEntityDataLambdas implements Function<ConductorElasticsearchApi, Boolean>, Serializable {

    private UUID                                         entitySetId;
    private Map<UUID, Map<UUID, Map<UUID, Set<Object>>>> entitiesByIdByEntitySetId;
    private boolean                                      linking;

    public BulkEntityDataLambdas(
            UUID entitySetId,
            Map<UUID, Map<UUID, Map<UUID, Set<Object>>>> entitiesByIdByEntitySetId,
            boolean linking ) {
        this.entitySetId = entitySetId;
        this.entitiesByIdByEntitySetId = entitiesByIdByEntitySetId;
        this.linking = linking;
    }

    @Override
    public Boolean apply( ConductorElasticsearchApi conductorElasticsearchApi ) {
        return conductorElasticsearchApi.createBulkEntityData( entitySetId, entitiesByIdByEntitySetId, linking );
    }

    public UUID getEntitySetId() {
        return entitySetId;
    }

    public Map<UUID, Map<UUID, Map<UUID, Set<Object>>>> getEntitiesByIdByEntitySetId() {
        return entitiesByIdByEntitySetId;
    }

    public boolean isLinking() { return linking; }
}
