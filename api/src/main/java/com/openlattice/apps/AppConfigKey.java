package com.openlattice.apps;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.UUID;

public class AppConfigKey {

    private final UUID appId;
    private final UUID organizationId;

    @JsonCreator
    public AppConfigKey(
            @JsonProperty( SerializationConstants.APP_ID ) UUID appId,
            @JsonProperty( SerializationConstants.ORGANIZATION_ID ) UUID organizationId ) {
        this.appId = appId;
        this.organizationId = organizationId;
    }

    @JsonProperty( SerializationConstants.APP_ID )
    public UUID getAppId() {
        return appId;
    }

    @JsonProperty( SerializationConstants.ORGANIZATION_ID )
    public UUID getOrganizationId() {
        return organizationId;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        AppConfigKey that = (AppConfigKey) o;
        return Objects.equals( appId, that.appId ) &&
                Objects.equals( organizationId, that.organizationId );
    }

    @Override public int hashCode() {
        return Objects.hash( appId, organizationId );
    }

    @Override public String toString() {
        return "AppConfigKey{" +
                "appId=" + appId +
                ", organizationId=" + organizationId +
                '}';
    }
}
