package com.openlattice.apps;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.securable.AbstractSecurableObject;
import com.openlattice.client.serialization.SerializationConstants;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class AppRole {

    private UUID                                            id;
    private String                                          name;
    private String                                          title;
    private String                                          description;
    private Map<Permission, Map<UUID, Optional<Set<UUID>>>> permissions;

    @JsonCreator
    public AppRole(
            @JsonProperty( SerializationConstants.ID_FIELD ) Optional<UUID> id,
            @JsonProperty( SerializationConstants.NAME_FIELD ) String name,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.PERMISSIONS )
                    Map<Permission, Map<UUID, Optional<Set<UUID>>>> permissions ) {

        Preconditions.checkArgument( StringUtils.isNotBlank( name ), "AppRole name cannot be blank" );

        this.id = id.orElse( UUID.randomUUID() );
        this.name = name;
        this.title = title;
        this.description = description.orElse( "" );
        this.permissions = permissions;
    }

    public AppRole(
            UUID id,
            String name,
            String title,
            Optional<String> description,
            Map<Permission, Map<UUID, Optional<Set<UUID>>>> permissions ) {
        this( Optional.of( id ), name, title, description, permissions );
    }

    @JsonProperty( SerializationConstants.ID_FIELD )
    public UUID getId() {
        return id;
    }

    @JsonProperty( SerializationConstants.NAME_FIELD )
    public String getName() {
        return name;
    }

    @JsonProperty( SerializationConstants.TITLE_FIELD )
    public String getTitle() {
        return title;
    }

    @JsonProperty( SerializationConstants.DESCRIPTION_FIELD )
    public String getDescription() {
        return description;
    }

    @JsonProperty( SerializationConstants.PERMISSIONS )
    public Map<Permission, Map<UUID, Optional<Set<UUID>>>> getPermissions() {
        return permissions;
    }

    public void setPermissions( Map<Permission, Map<UUID, Optional<Set<UUID>>>> permissions ) {
        this.permissions = permissions;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        AppRole appRole = (AppRole) o;
        return Objects.equals( id, appRole.id ) &&
                Objects.equals( name, appRole.name ) &&
                Objects.equals( title, appRole.title ) &&
                Objects.equals( description, appRole.description ) &&
                Objects.equals( permissions, appRole.permissions );
    }

    @Override public int hashCode() {
        return Objects.hash( id, name, title, description, permissions );
    }

    @Override public String toString() {
        return "AppRole{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", title='" + title + '\'' +
                ", description='" + description + '\'' +
                ", permissions=" + permissions +
                '}';
    }
}
