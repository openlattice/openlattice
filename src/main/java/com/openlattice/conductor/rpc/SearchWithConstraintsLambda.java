package com.openlattice.conductor.rpc;

import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;
import com.openlattice.search.requests.EntityDataKeySearchResult;
import com.openlattice.search.requests.SearchConstraints;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

public class SearchWithConstraintsLambda
        implements Function<ConductorElasticsearchApi, EntityDataKeySearchResult>, Serializable {

    private SearchConstraints           searchConstraints;
    private Map<UUID, UUID>             entityTypesByEntitySetId;
    private Map<UUID, DelegatedUUIDSet> authorizedProperties;
    private Map<UUID, DelegatedUUIDSet> linkingEntitySets;

    public SearchWithConstraintsLambda(
            SearchConstraints searchConstraints,
            Map<UUID, UUID> entityTypesByEntitySetId,
            Map<UUID, DelegatedUUIDSet> authorizedProperties,
            Map<UUID, DelegatedUUIDSet> linkingEntitySets ) {
        this.searchConstraints = searchConstraints;
        this.entityTypesByEntitySetId = entityTypesByEntitySetId;
        this.authorizedProperties = authorizedProperties;
        this.linkingEntitySets = linkingEntitySets;
    }

    @Override public EntityDataKeySearchResult apply( ConductorElasticsearchApi conductorElasticsearchApi ) {
        return conductorElasticsearchApi
                .executeSearch( searchConstraints, entityTypesByEntitySetId, authorizedProperties, linkingEntitySets );
    }

    public SearchConstraints getSearchConstraints() {
        return searchConstraints;
    }

    public Map<UUID, UUID> getEntityTypesByEntitySetId() {
        return entityTypesByEntitySetId;
    }

    public Map<UUID, DelegatedUUIDSet> getAuthorizedProperties() {
        return authorizedProperties;
    }

    public Map<UUID, DelegatedUUIDSet> getLinkingEntitySets() {
        return linkingEntitySets;
    }
}
