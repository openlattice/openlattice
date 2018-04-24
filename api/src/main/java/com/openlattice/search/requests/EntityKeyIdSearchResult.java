package com.openlattice.search.requests;

import java.util.List;
import java.util.UUID;

public class EntityKeyIdSearchResult {

    private final long       numHits;
    private final List<UUID> entityKeyIds;

    public EntityKeyIdSearchResult( long numHits, List<UUID> entityKeyIds ) {
        this.numHits = numHits;
        this.entityKeyIds = entityKeyIds;
    }

    public long getNumHits() {
        return numHits;
    }

    public List<UUID> getEntityKeyIds() {
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
