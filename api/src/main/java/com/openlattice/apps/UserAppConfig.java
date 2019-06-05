package com.openlattice.apps;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public class UserAppConfig {

    private final UUID      organizationId;
    private final UUID      entitySetCollectionId;
    private final Set<UUID> roles;

    @JsonCreator
    public UserAppConfig(
            @JsonProperty( SerializationConstants.ORGANIZATION_ID ) UUID organizationId,
            @JsonProperty( SerializationConstants.ENTITY_SET_COLLECTION_ID) UUID entitySetCollectionId,
            @JsonProperty( SerializationConstants.ROLES ) Set<UUID> roles ) {
        this.organizationId = organizationId;
        this.entitySetCollectionId = entitySetCollectionId;
        this.roles = roles;
    }

    @JsonProperty( SerializationConstants.ORGANIZATION_ID)
    public UUID getOrganizationId() {
        return organizationId;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_COLLECTION_ID)
    public UUID getEntitySetCollectionId() {
        return entitySetCollectionId;
    }

    @JsonProperty( SerializationConstants.ROLES)
    public Set<UUID> getRoles() {
        return roles;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        UserAppConfig that = (UserAppConfig) o;
        return Objects.equals( organizationId, that.organizationId ) &&
                Objects.equals( entitySetCollectionId, that.entitySetCollectionId ) &&
                Objects.equals( roles, that.roles );
    }

    @Override
    public int hashCode() {
        return Objects.hash( organizationId, entitySetCollectionId, roles );
    }

    @Override
    public String toString() {
        return "UserAppConfig{" +
                "organizationId=" + organizationId +
                ", entitySetCollectionId=" + entitySetCollectionId +
                ", roles=" + roles +
                '}';
    }
}
