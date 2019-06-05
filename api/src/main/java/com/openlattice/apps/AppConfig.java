package com.openlattice.apps;

import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.organization.Organization;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.authorization.SecurablePrincipal;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public class AppConfig extends SecurablePrincipal {

    private final UUID              appId;
    private final UUID              organizationId;
    private final UUID              entitySetCollectionId;
    private final Map<UUID, AclKey> roles;

    @JsonCreator
    public AppConfig(
            @JsonProperty( SerializationConstants.ID_FIELD ) Optional<UUID> id,
            @JsonProperty( SerializationConstants.PRINCIPAL ) Principal principal,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.APP_ID ) UUID appId,
            @JsonProperty( SerializationConstants.ORGANIZATION_ID ) UUID organizationId,
            @JsonProperty( SerializationConstants.ENTITY_SET_COLLECTION_ID ) UUID entitySetCollectionId,
            @JsonProperty( SerializationConstants.ROLES ) Map<UUID, AclKey> roles ) {
        super( id, principal, title, description );
        this.appId = appId;
        this.organizationId = organizationId;
        this.entitySetCollectionId = entitySetCollectionId;
        this.roles = roles;
    }

    public AppConfig(
            Optional<UUID> id,
            String title,
            Optional<String> description,
            UUID appId,
            UUID organizationId,
            UUID entitySetCollectionId,
            Map<UUID, AclKey> roles ) {
        this( id,
                new Principal( PrincipalType.APP, getAppPrincipalId( appId, organizationId ) ),
                title,
                description,
                appId,
                organizationId,
                entitySetCollectionId,
                roles );
    }

    @JsonProperty( SerializationConstants.APP_ID )
    public UUID getAppId() {
        return appId;
    }

    @JsonProperty( SerializationConstants.ORGANIZATION_ID )
    public UUID getOrganizationId() {
        return organizationId;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_COLLECTION_ID )
    public UUID getEntitySetCollectionId() {
        return entitySetCollectionId;
    }

    @JsonProperty( SerializationConstants.ROLES )
    public Map<UUID, AclKey> getRoles() {
        return roles;
    }

    public static String getAppPrincipalId( UUID appId, UUID organizationId ) {
        return appId.toString().concat( "|" ).concat( organizationId.toString() );
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        if ( !super.equals( o ) )
            return false;
        AppConfig appConfig = (AppConfig) o;
        return Objects.equals( appId, appConfig.appId ) &&
                Objects.equals( organizationId, appConfig.organizationId ) &&
                Objects.equals( entitySetCollectionId, appConfig.entitySetCollectionId ) &&
                Objects.equals( roles, appConfig.roles );
    }

    @Override public int hashCode() {
        return Objects.hash( super.hashCode(), appId, organizationId, entitySetCollectionId, roles );
    }

    @Override public String toString() {
        return "AppConfig{" +
                "appId=" + appId +
                ", organizationId=" + organizationId +
                ", entitySetCollectionId=" + entitySetCollectionId +
                ", roles=" + roles +
                ", id=" + id +
                ", title='" + title + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
