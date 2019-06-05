package com.openlattice.apps;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.openlattice.client.serialization.SerializationConstants;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public class AppInstallation {

    private Optional<UUID>                entitySetCollectionId;
    private Optional<String>              prefix;
    private Optional<Map<UUID, UUID>>     template;
    private Optional<Map<String, Object>> settings;

    @JsonCreator
    public AppInstallation(
            @JsonProperty( SerializationConstants.ENTITY_SET_COLLECTION_ID ) Optional<UUID> entitySetCollectionId,
            @JsonProperty( SerializationConstants.PREFIX ) Optional<String> prefix,
            @JsonProperty( SerializationConstants.TEMPLATE ) Optional<Map<UUID, UUID>> template,
            @JsonProperty( SerializationConstants.SETTINGS ) Optional<Map<String, Object>> settings ) {

        Preconditions.checkArgument( entitySetCollectionId.isPresent() || template.isPresent(),
                "entitySetCollectionId and template cannot both be empty for AppInstallation" );

        if ( entitySetCollectionId.isPresent() ) {
            Preconditions.checkArgument( prefix.isEmpty() && template.isEmpty(),
                    "If AppInstallation specifies entitySetCollectionId, prefix and template must be empty." );
        }

        if ( prefix.isPresent() ) {
            Preconditions
                    .checkArgument( template.isPresent(), "AppInstallation cannot specify prefix but not template." );
        } else if ( template.isPresent() ) {
            Preconditions
                    .checkArgument( template.isPresent(), "AppInstallation cannot specify template but not prefix." );
        }

        this.entitySetCollectionId = entitySetCollectionId;
        this.prefix = prefix;
        this.template = template;
        this.settings = settings;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_COLLECTION_ID )
    public Optional<UUID> getEntitySetCollectionId() {
        return entitySetCollectionId;
    }

    @JsonProperty( SerializationConstants.PREFIX )
    public Optional<String> getPrefix() {
        return prefix;
    }

    @JsonProperty( SerializationConstants.TEMPLATE )
    public Optional<Map<UUID, UUID>> getTemplate() {
        return template;
    }

    @JsonProperty( SerializationConstants.SETTINGS )
    public Optional<Map<String, Object>> getSettings() {
        return settings;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        AppInstallation that = (AppInstallation) o;
        return Objects.equals( entitySetCollectionId, that.entitySetCollectionId ) &&
                Objects.equals( prefix, that.prefix ) &&
                Objects.equals( template, that.template ) &&
                Objects.equals( settings, that.settings );
    }

    @Override public int hashCode() {
        return Objects.hash( entitySetCollectionId, prefix, template, settings );
    }

    @Override public String toString() {
        return "AppInstallation{" +
                "entitySetCollectionId=" + entitySetCollectionId +
                ", prefix=" + prefix +
                ", template=" + template +
                ", settings=" + settings +
                '}';
    }
}
