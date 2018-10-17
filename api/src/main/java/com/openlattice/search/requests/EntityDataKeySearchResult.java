package com.openlattice.search.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.data.EntityDataKey;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public class EntityDataKeySearchResult {

    private final long               numHits;
    private final Set<EntityDataKey> entityDataKeys;

    public EntityDataKeySearchResult(
            @JsonProperty( SerializationConstants.NUM_HITS ) long numHits,
            @JsonProperty( SerializationConstants.HITS ) Set<EntityDataKey> entityDataKeys ) {
        this.numHits = numHits;
        this.entityDataKeys = entityDataKeys;
    }

    @JsonProperty( SerializationConstants.NUM_HITS )
    public long getNumHits() {
        return numHits;
    }

    @JsonProperty( SerializationConstants.HITS )
    public Set<EntityDataKey> getEntityDataKeys() {
        return entityDataKeys;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        EntityDataKeySearchResult that = (EntityDataKeySearchResult) o;
        return numHits == that.numHits &&
                Objects.equals( entityDataKeys, that.entityDataKeys );
    }

    @Override public int hashCode() {

        return Objects.hash( numHits, entityDataKeys );
    }
}
