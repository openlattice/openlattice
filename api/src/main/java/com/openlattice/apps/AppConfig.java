package com.openlattice.apps;

import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.organization.Organization;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.authorization.SecurablePrincipal;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class AppConfig extends SecurablePrincipal {

    private final UUID                        appId;
    private final Organization                organization;
    private final Map<String, AppTypeSetting> config;

    @JsonCreator
    public AppConfig(
            @JsonProperty( SerializationConstants.ID_FIELD ) Optional<UUID> id,
            @JsonProperty( SerializationConstants.PRINCIPAL ) Principal principal,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.APP_ID ) UUID appId,
            @JsonProperty( SerializationConstants.ORGANIZATION ) Organization organization,
            @JsonProperty( SerializationConstants.CONFIG ) Map<String, AppTypeSetting> config ) {
        super( id, principal, title, description );
        this.appId = appId;
        this.organization = organization;
        this.config = config;
    }

    public AppConfig(
            Optional<UUID> id,
            String title,
            Optional<String> description,
            UUID appId,
            Organization organization,
            Map<String, AppTypeSetting> config ) {
        this( id,
                new Principal( PrincipalType.APP, getAppPrincipalId( appId, organization.getId() ) ),
                title,
                description,
                appId,
                organization,
                config );
    }

    @JsonProperty( SerializationConstants.APP_ID )
    public UUID getAppId() {
        return appId;
    }

    @JsonProperty( SerializationConstants.ORGANIZATION )
    public Organization getOrganization() {
        return organization;
    }

    @JsonProperty( SerializationConstants.CONFIG )
    public Map<String, AppTypeSetting> getConfig() {
        return config;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        AppConfig appConfig = (AppConfig) o;

        if ( !organization.equals( appConfig.organization ) )
            return false;
        return config.equals( appConfig.config );
    }

    @Override public int hashCode() {
        int result = organization.hashCode();
        result = 31 * result + config.hashCode();
        return result;
    }

    public static String getAppPrincipalId( UUID appId, UUID organizationId ) {
        return appId.toString().concat( "|" ).concat( organizationId.toString() );
    }
}
