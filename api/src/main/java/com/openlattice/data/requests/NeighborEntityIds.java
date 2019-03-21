package com.openlattice.data.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;

import java.util.Objects;
import java.util.UUID;

public class NeighborEntityIds {

    private UUID associationEntityKeyId;
    private UUID neighborEntityKeyId;
    private boolean isSrc;

    @JsonCreator
    public NeighborEntityIds(
            @JsonProperty( SerializationConstants.ASSOCIATION_ID ) UUID associationEntityKeyId,
            @JsonProperty( SerializationConstants.NEIGHBOR_ID ) UUID neighborEntityKeyId,
            @JsonProperty( SerializationConstants.SRC ) boolean isSrc ) {
        this.associationEntityKeyId = associationEntityKeyId;
        this.neighborEntityKeyId = neighborEntityKeyId;
        this.isSrc = isSrc;
    }

    @JsonProperty( SerializationConstants.ASSOCIATION_ID )
    public UUID getAssociationEntityKeyId() {
        return associationEntityKeyId;
    }

    @JsonProperty( SerializationConstants.NEIGHBOR_ID )
    public UUID getNeighborEntityKeyId() {
        return neighborEntityKeyId;
    }

    @JsonProperty( SerializationConstants.SRC )
    public boolean isSrc() {
        return isSrc;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        NeighborEntityIds that = (NeighborEntityIds) o;
        return isSrc == that.isSrc &&
                Objects.equals( associationEntityKeyId, that.associationEntityKeyId ) &&
                Objects.equals( neighborEntityKeyId, that.neighborEntityKeyId );
    }

    @Override public int hashCode() {
        return Objects.hash( associationEntityKeyId, neighborEntityKeyId, isSrc );
    }

    @Override public String toString() {
        return "NeighborEntityIds{" +
                "associationEntityKeyId=" + associationEntityKeyId +
                ", neighborEntityKeyId=" + neighborEntityKeyId +
                ", isSrc=" + isSrc +
                '}';
    }
}
