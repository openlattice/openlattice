package com.openlattice.apps;

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.authorization.securable.AbstractSecurableObject;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.client.serialization.SerializationConstants;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;

public class  App extends AbstractSecurableObject {

    private final LinkedHashSet<UUID> appTypeIds;
    private       String              name;
    private       String              url;

    @JsonCreator
    public App(
            @JsonProperty( SerializationConstants.ID_FIELD ) Optional<UUID> id,
            @JsonProperty( SerializationConstants.NAME_FIELD ) String name,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.APP_TYPE_IDS_FIELD ) LinkedHashSet<UUID> appTypeIds,
            @JsonProperty( SerializationConstants.URL ) String url ) {
        super( id, title, description );
        checkArgument( StringUtils.isNotBlank( name ), "App name cannot be blank." );
        this.name = name;
        this.appTypeIds = appTypeIds;
        this.url = url;
    }

    public App(
            UUID id,
            String name,
            String title,
            Optional<String> description,
            LinkedHashSet<UUID> configTypeIds,
            String url ) {
        this( Optional.of( id ), name, title, description, configTypeIds, url );
    }

    public App(
            String name,
            String title,
            Optional<String> description,
            LinkedHashSet<UUID> configTypeIds,
            String url ) {
        this( Optional.empty(), name, title, description, configTypeIds, url );
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

    @JsonIgnore
    @Override public SecurableObjectType getCategory() {
        return SecurableObjectType.App;
    }

    @JsonProperty( SerializationConstants.APP_TYPE_IDS_FIELD )
    public LinkedHashSet<UUID> getAppTypeIds() {
        return appTypeIds;
    }

    public void addAppTypeIds( Set<UUID> appTypeIds ) {
        this.appTypeIds.addAll( appTypeIds );
    }

    public void removeAppTypeIds( Set<UUID> appTypeIds ) {
        this.appTypeIds.removeAll( appTypeIds );
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( o == null || getClass() != o.getClass() ) { return false; }
        if ( !super.equals( o ) ) { return false; }

        App app = (App) o;

        if ( name != null ? !name.equals( app.name ) : app.name != null ) { return false; }
        if ( appTypeIds != null ? !appTypeIds.equals( app.appTypeIds ) : app.appTypeIds != null ) { return false; }
        return url != null ? url.equals( app.url ) : app.url == null;
    }

    @Override public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + ( name != null ? name.hashCode() : 0 );
        result = 31 * result + ( appTypeIds != null ? appTypeIds.hashCode() : 0 );
        result = 31 * result + ( url != null ? url.hashCode() : 0 );
        return result;
    }

    @Override public String toString() {
        return "App{" +
                "name='" + name + '\'' +
                ", appTypeIds=" + appTypeIds +
                ", id=" + id +
                ", title='" + title + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
