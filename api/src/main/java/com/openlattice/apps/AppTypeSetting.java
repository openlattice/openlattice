package com.openlattice.apps;

import com.openlattice.authorization.Permission;
import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.EnumSet;
import java.util.UUID;

public class AppTypeSetting {
    private UUID entitySetId;
    private EnumSet<Permission> permissions;

    @JsonCreator
    public AppTypeSetting(
            @JsonProperty( SerializationConstants.ENTITY_SET_ID ) UUID entitySetId,
            @JsonProperty( SerializationConstants.PERMISSIONS ) EnumSet<Permission> permissions ) {
        this.entitySetId = entitySetId;
        this.permissions = permissions;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_ID )
    public UUID getEntitySetId() {
        return entitySetId;
    }

    @JsonProperty( SerializationConstants.PERMISSIONS )
    public EnumSet<Permission> getPermissions() {
        return permissions;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        AppTypeSetting that = (AppTypeSetting) o;

        if ( !entitySetId.equals( that.entitySetId ) )
            return false;
        return permissions.equals( that.permissions );
    }

    public void setEntitySetId( UUID entitySetId ) {
        this.entitySetId = entitySetId;
    }

    public void setPermissions( EnumSet<Permission> permissions ) {
        this.permissions = permissions;
    }

    @Override public int hashCode() {
        int result = entitySetId.hashCode();
        result = 31 * result + permissions.hashCode();
        return result;
    }

    @Override public String toString() {
        return "AppTypeSetting{" +
                "entitySetId=" + entitySetId +
                ", permissions=" + permissions +
                '}';
    }
}
