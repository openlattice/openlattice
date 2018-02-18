package com.openlattice.apps;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

public class AppConfigKey {

    private final UUID appId;
    private final UUID organizationId;
    private final UUID appTypeId;

    @JsonCreator
    public AppConfigKey(
            @JsonProperty( SerializationConstants.APP_ID ) UUID appId,
            @JsonProperty( SerializationConstants.ORGANIZATION_ID ) UUID organizationId,
            @JsonProperty( SerializationConstants.APP_TYPE_ID ) UUID appTypeId ) {
        this.appId = appId;
        this.organizationId = organizationId;
        this.appTypeId = appTypeId;
    }

    @JsonProperty( SerializationConstants.APP_ID )
    public UUID getAppId() {
        return appId;
    }

    @JsonProperty( SerializationConstants.ORGANIZATION_ID )
    public UUID getOrganizationId() {
        return organizationId;
    }

    @JsonProperty( SerializationConstants.APP_TYPE_ID )
    public UUID getAppTypeId() {
        return appTypeId;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        AppConfigKey that = (AppConfigKey) o;

        if ( !appId.equals( that.appId ) )
            return false;
        if ( !organizationId.equals( that.organizationId ) )
            return false;
        return appTypeId.equals( that.appTypeId );
    }

    @Override public int hashCode() {
        int result = appId.hashCode();
        result = 31 * result + organizationId.hashCode();
        result = 31 * result + appTypeId.hashCode();
        return result;
    }
}
