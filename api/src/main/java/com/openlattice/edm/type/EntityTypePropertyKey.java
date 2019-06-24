package com.openlattice.edm.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;

import java.util.UUID;

public class EntityTypePropertyKey {
    private UUID entityTypeId;
    private UUID propertyTypeId;

    @JsonCreator
    public EntityTypePropertyKey(
            @JsonProperty( SerializationConstants.ENTITY_TYPE_ID ) UUID entityTypeId,
            @JsonProperty( SerializationConstants.PROPERTY_TYPE_ID ) UUID propertyTypeId ) {
        this.entityTypeId = entityTypeId;
        this.propertyTypeId = propertyTypeId;
    }

    @JsonProperty( SerializationConstants.ENTITY_TYPE_ID )
    public UUID getEntityTypeId() {
        return entityTypeId;
    }

    @JsonProperty( SerializationConstants.PROPERTY_TYPE_ID )
    public UUID getPropertyTypeId() {
        return propertyTypeId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( entityTypeId == null ) ? 0 : entityTypeId.hashCode() );
        result = prime * result + ( ( propertyTypeId == null ) ? 0 : propertyTypeId.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        EntityTypePropertyKey other = (EntityTypePropertyKey) obj;
        if ( entityTypeId == null ) {
            if ( other.entityTypeId != null ) return false;
        } else if ( !entityTypeId.equals( other.entityTypeId ) ) return false;
        if ( propertyTypeId == null ) {
            if ( other.propertyTypeId != null ) return false;
        } else if ( !propertyTypeId.equals( other.propertyTypeId ) ) return false;
        return true;
    }

}
