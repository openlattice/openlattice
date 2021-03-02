package com.openlattice.search.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.openlattice.client.serialization.SerializationConstants;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class EntityNeighborsFilter {

    private Set<UUID>           entityKeyIds;
    private Optional<Set<UUID>> srcEntitySetIds;
    private Optional<Set<UUID>> dstEntitySetIds;
    private Optional<Set<UUID>> associationEntitySetIds;

    @JsonCreator
    public EntityNeighborsFilter(
            @JsonProperty( SerializationConstants.ENTITY_KEY_IDS ) Set<UUID> entityKeyIds,
            @JsonProperty( SerializationConstants.SRC ) Optional<Set<UUID>> srcEntitySetIds,
            @JsonProperty( SerializationConstants.DST ) Optional<Set<UUID>> dstEntitySetIds,
            @JsonProperty( SerializationConstants.EDGE ) Optional<Set<UUID>> associationEntitySetIds ) {
        Preconditions.checkArgument( entityKeyIds.size() > 0, "entityKeyIds cannot be empty" );
        this.entityKeyIds = entityKeyIds;
        this.srcEntitySetIds = srcEntitySetIds;
        this.dstEntitySetIds = dstEntitySetIds;
        this.associationEntitySetIds = associationEntitySetIds;
    }

    public EntityNeighborsFilter( Set<UUID> entityKeyIds ) {
        this( entityKeyIds, Optional.empty(), Optional.empty(), Optional.empty() );
    }

    @JsonProperty( SerializationConstants.ENTITY_KEY_IDS )
    public Set<UUID> getEntityKeyIds() {
        return entityKeyIds;
    }

    @JsonProperty( SerializationConstants.SRC )
    public Optional<Set<UUID>> getSrcEntitySetIds() {
        return srcEntitySetIds;
    }

    @JsonProperty( SerializationConstants.DST )
    public Optional<Set<UUID>> getDstEntitySetIds() {
        return dstEntitySetIds;
    }

    @JsonProperty( SerializationConstants.EDGE )
    public Optional<Set<UUID>> getAssociationEntitySetIds() {
        return associationEntitySetIds;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        EntityNeighborsFilter that = (EntityNeighborsFilter) o;
        return Objects.equals( entityKeyIds, that.entityKeyIds ) &&
                Objects.equals( srcEntitySetIds, that.srcEntitySetIds ) &&
                Objects.equals( dstEntitySetIds, that.dstEntitySetIds ) &&
                Objects.equals( associationEntitySetIds, that.associationEntitySetIds );
    }

    @Override public int hashCode() {

        return Objects.hash( entityKeyIds, srcEntitySetIds, dstEntitySetIds, associationEntitySetIds );
    }
}
