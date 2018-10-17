package com.openlattice.search.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public class EntityKeyIdSearchResult {

    private final long       numHits;
    private final Set<UUID> entityKeyIds;

    public EntityKeyIdSearchResult( @JsonProperty( SerializationConstants.NUM_HITS ) long numHits,
            @JsonProperty( SerializationConstants.HITS ) Set<UUID> entityKeyIds ) {
        this.numHits = numHits;
        this.entityKeyIds = entityKeyIds;
    }

    @JsonProperty( SerializationConstants.NUM_HITS )
    public long getNumHits() {
        return numHits;
    }

    @JsonProperty( SerializationConstants.HITS )
    public Set<UUID> getEntityKeyIds() {
        return entityKeyIds;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        EntityKeyIdSearchResult that = (EntityKeyIdSearchResult) o;

        if ( numHits != that.numHits )
            return false;
        return entityKeyIds != null ? entityKeyIds.equals( that.entityKeyIds ) : that.entityKeyIds == null;
    }

    @Override
    public int hashCode() {
        int result = (int) ( numHits ^ ( numHits >>> 32 ) );
        result = 31 * result + ( entityKeyIds != null ? entityKeyIds.hashCode() : 0 );
        return result;
    }
}
