package com.openlattice.apps;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.Principal;
import com.openlattice.client.serialization.SerializationConstants;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public class AppTypeSetting {

    private UUID      id;
    private UUID      entitySetCollectionId;
    private Map<UUID, AclKey> roles;
    private Map<String, Object> settings;

    @JsonCreator
    public AppTypeSetting(
            @JsonProperty( SerializationConstants.ID_FIELD ) UUID id,
            @JsonProperty( SerializationConstants.ENTITY_SET_COLLECTION_ID ) UUID entitySetCollectionId,
            @JsonProperty( SerializationConstants.ROLES ) Map<UUID, AclKey> roles,
            @JsonProperty( SerializationConstants.SETTINGS) Map<String, Object> settings ) {
        this.id = id;
        this.entitySetCollectionId = entitySetCollectionId;
        this.roles = roles;
        this.settings = settings;
    }

    @JsonProperty( SerializationConstants.ID_FIELD )
    public UUID getId() {
        return id;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_COLLECTION_ID )
    public UUID getEntitySetCollectionId() {
        return entitySetCollectionId;
    }

    @JsonProperty( SerializationConstants.ROLES )
    public Map<UUID, AclKey> getRoles() {
        return roles;
    }

    @JsonProperty( SerializationConstants.SETTINGS )
    public Map<String, Object> getSettings() {
        return settings;
    }

    public void updateSettings( Map<String, Object> settingsUpdates ) {
        settings.putAll( settingsUpdates );
    }

    public void removeSettings( Set<String> settingsKeys ) {
        settingsKeys.forEach( settings::remove );
    }

    public void addRole( UUID roleId, AclKey roleAclKey ) {
        roles.put( roleId, roleAclKey );
    }

    public void removeRole( UUID roleId ) {
        roles.remove( roleId );
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        AppTypeSetting that = (AppTypeSetting) o;
        return Objects.equals( id, that.id ) &&
                Objects.equals( entitySetCollectionId, that.entitySetCollectionId ) &&
                Objects.equals( roles, that.roles ) &&
                Objects.equals( settings, that.settings );
    }

    @Override public int hashCode() {
        return Objects.hash( id, entitySetCollectionId, roles, settings );
    }

    @Override public String toString() {
        return "AppTypeSetting{" +
                "id=" + id +
                ", entitySetCollectionId=" + entitySetCollectionId +
                ", roles=" + roles +
                ", settings=" + settings +
                '}';
    }
}
