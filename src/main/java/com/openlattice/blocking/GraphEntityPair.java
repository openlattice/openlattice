package com.openlattice.blocking;

import java.io.Serializable;
import java.util.UUID;

public class GraphEntityPair implements Serializable {
    private static final long serialVersionUID = -6371783971935966458L;

    private final UUID graphId;
    private final UUID entityKeyId;

    public GraphEntityPair( UUID graphId, UUID entityKeyId ) {
        this.graphId = graphId;
        this.entityKeyId = entityKeyId;
    }

    public UUID getGraphId() {
        return graphId;
    }

    public UUID getEntityKeyId() {
        return entityKeyId;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        GraphEntityPair that = (GraphEntityPair) o;

        if ( graphId != null ? !graphId.equals( that.graphId ) : that.graphId != null )
            return false;
        return entityKeyId != null ? entityKeyId.equals( that.entityKeyId ) : that.entityKeyId == null;
    }

    @Override public int hashCode() {
        int result = graphId != null ? graphId.hashCode() : 0;
        result = 31 * result + ( entityKeyId != null ? entityKeyId.hashCode() : 0 );
        return result;
    }
}
