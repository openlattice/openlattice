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
    private Map<UUID, DelegatedUUIDSet> authorizedProperties;
    private boolean                     linking;

    public SearchWithConstraintsLambda(
            SearchConstraints searchConstraints,
            Map<UUID, DelegatedUUIDSet> authorizedProperties,
            boolean linkign ) {
        this.searchConstraints = searchConstraints;
        this.authorizedProperties = authorizedProperties;
        this.linking = linkign;
    }

    @Override public EntityDataKeySearchResult apply( ConductorElasticsearchApi conductorElasticsearchApi ) {
        return conductorElasticsearchApi.executeSearch( searchConstraints, authorizedProperties, linking );
    }

    public SearchConstraints getSearchConstraints() {
        return searchConstraints;
    }

    public Map<UUID, DelegatedUUIDSet> getAuthorizedProperties() {
        return authorizedProperties;
    }

    public boolean isLinking() { return linking; }
}
