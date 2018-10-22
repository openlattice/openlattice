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

    public SearchWithConstraintsLambda(
            SearchConstraints searchConstraints,
            Map<UUID, DelegatedUUIDSet> authorizedProperties ) {
        this.searchConstraints = searchConstraints;
        this.authorizedProperties = authorizedProperties;
    }

    @Override public EntityDataKeySearchResult apply( ConductorElasticsearchApi conductorElasticsearchApi ) {
        return conductorElasticsearchApi.executeSearch( searchConstraints, authorizedProperties );
    }

    public SearchConstraints getSearchConstraints() {
        return searchConstraints;
    }

    public Map<UUID, DelegatedUUIDSet> getAuthorizedProperties() {
        return authorizedProperties;
    }
}
