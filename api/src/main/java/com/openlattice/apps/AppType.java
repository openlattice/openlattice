package com.openlattice.apps;

import com.openlattice.authorization.securable.AbstractSecurableType;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.UUID;

public class AppType extends AbstractSecurableType {

    private final UUID entityTypeId;

    @JsonCreator
    public AppType(
            @JsonProperty( SerializationConstants.ID_FIELD ) Optional<UUID> id,
            @JsonProperty( SerializationConstants.TYPE_FIELD ) FullQualifiedName type,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.ENTITY_TYPE_ID_FIELD ) UUID entityTypeId ) {
        super( id, type, title, description );
        Preconditions.checkNotNull( entityTypeId, "entityTypeId cannot be null for an AppType" );
        this.entityTypeId = entityTypeId;
    }

    public AppType( UUID id, FullQualifiedName type, String title, Optional<String> description, UUID entityTypeId ) {
        this( Optional.of( id ), type, title, description, entityTypeId );
    }

    public AppType( FullQualifiedName type, String title, Optional<String> description, UUID entityTypeId ) {
        this( Optional.absent(), type, title, description, entityTypeId );
    }

    @Override public SecurableObjectType getCategory() {
        return SecurableObjectType.AppType;
    }

    @JsonProperty( SerializationConstants.ENTITY_TYPE_ID_FIELD )
    public UUID getEntityTypeId() {
        return entityTypeId;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        if ( !super.equals( o ) )
            return false;

        AppType appType = (AppType) o;

        return entityTypeId.equals( appType.entityTypeId );
    }

    @Override public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + entityTypeId.hashCode();
        return result;
    }

    @Override public String toString() {
        return "AppType{" +
                "entityTypeId=" + entityTypeId +
                ", type=" + type +
                ", id=" + id +
                ", title='" + title + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
