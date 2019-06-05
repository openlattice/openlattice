package com.openlattice.apps;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.securable.AbstractSecurableObject;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.client.serialization.SerializationConstants;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class App extends AbstractSecurableObject {

    private final UUID                entityTypeCollectionId;
    private       String              name;
    private       String              url;
    private       Set<AppRole>        appRoles;
    private       Map<String, Object> defaultSettings;

    @JsonCreator
    public App(
            @JsonProperty( SerializationConstants.ID_FIELD ) Optional<UUID> id,
            @JsonProperty( SerializationConstants.NAME_FIELD ) String name,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.URL ) String url,
            @JsonProperty( SerializationConstants.ENTITY_TYPE_COLLECTION_ID ) UUID entityTypeCollectionId,
            @JsonProperty( SerializationConstants.ROLES ) Set<AppRole> appRoles,
            @JsonProperty( SerializationConstants.SETTINGS ) Optional<Map<String, Object>> defaultSettings ) {
        super( id, title, description );
        checkArgument( StringUtils.isNotBlank( name ), "App name cannot be blank." );
        this.name = name;
        this.entityTypeCollectionId = entityTypeCollectionId;
        this.url = url;
        this.appRoles = appRoles;
        this.defaultSettings = defaultSettings.orElse( ImmutableMap.of() );
    }

    public App(
            UUID id,
            String name,
            String title,
            Optional<String> description,
            String url,
            UUID entityTypeCollectionId,
            Set<AppRole> appRoles,
            Optional<Map<String, Object>> defaultSettings ) {
        this( Optional.of( id ), name, title, description, url, entityTypeCollectionId, appRoles, defaultSettings );
    }

    public App(
            String name,
            String title,
            Optional<String> description,
            String url,
            UUID entityTypeCollectionId,
            Set<AppRole> appRoles,
            Optional<Map<String, Object>> defaultSettings ) {
        this( Optional.empty(), name, title, description, url, entityTypeCollectionId, appRoles, defaultSettings );
    }

    @JsonProperty( SerializationConstants.NAME_FIELD )
    public String getName() {
        return name;
    }

    public void setName( String name ) {
        this.name = name;
    }

    @JsonProperty( SerializationConstants.URL )
    public String getUrl() {
        return url;
    }

    public void setUrl( String url ) {
        this.url = url;
    }

    @JsonProperty( SerializationConstants.ENTITY_TYPE_COLLECTION_ID )
    public UUID getEntityTypeCollectionId() {
        return entityTypeCollectionId;
    }

    @JsonProperty( SerializationConstants.ROLES )
    public Set<AppRole> getAppRoles() {
        return appRoles;
    }

    @JsonProperty( SerializationConstants.SETTINGS )
    public Map<String, Object> getDefaultSettings() {
        return defaultSettings;
    }

    public void addRole( AppRole role ) {
        appRoles.add( role );
    }

    public void removeRole( UUID roleId ) {
        appRoles.removeIf( r -> r.getId().equals( roleId ) );
    }

    public void setDefaultSettings( Map<String, Object> defaultSettings ) {
        this.defaultSettings = defaultSettings;
    }

    public void setRolePermissions( UUID roleId, Map<Permission, Map<UUID, Optional<Set<UUID>>>> permissions ) {
        appRoles = appRoles.stream().map( role -> {
            if ( role.getId().equals( roleId ) ) {
                role.setPermissions( permissions );
                return role;
            }
            return role;
        } ).collect( Collectors.toSet() );

    }

    @JsonIgnore
    @Override public SecurableObjectType getCategory() {
        return SecurableObjectType.App;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        if ( !super.equals( o ) )
            return false;
        App app = (App) o;
        return Objects.equals( entityTypeCollectionId, app.entityTypeCollectionId ) &&
                Objects.equals( name, app.name ) &&
                Objects.equals( url, app.url ) &&
                Objects.equals( appRoles, app.appRoles ) &&
                Objects.equals( defaultSettings, app.defaultSettings );
    }

    @Override public int hashCode() {
        return Objects.hash( super.hashCode(), entityTypeCollectionId, name, url, appRoles, defaultSettings );
    }

    @Override public String toString() {
        return "App{" +
                "entityTypeCollectionId=" + entityTypeCollectionId +
                ", name='" + name + '\'' +
                ", url='" + url + '\'' +
                ", appRoles=" + appRoles +
                ", defaultSettings=" + defaultSettings +
                ", id=" + id +
                ", title='" + title + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
