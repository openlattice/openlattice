package com.openlattice.edm.collection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public class CollectionTemplateType {

    private final UUID   id;
    private       String name;
    private       String title;
    private       String description;
    private final UUID   entityTypeId;

    @JsonCreator
    public CollectionTemplateType(
            @JsonProperty( SerializationConstants.ID_FIELD ) Optional<UUID> id,
            @JsonProperty( SerializationConstants.NAME ) String name,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.ENTITY_TYPE_ID ) UUID entityTypeId ) {
        this.id = id.orElse( UUID.randomUUID() );
        this.name = name;
        this.title = title;
        this.description = description.orElse( "" );
        this.entityTypeId = entityTypeId;
    }

    public CollectionTemplateType(
            UUID id,
            String name,
            String title,
            Optional<String> description,
            UUID entityTypeId ) {
        this( Optional.of( id ), name, title, description, entityTypeId );
    }

    @JsonProperty( SerializationConstants.ID_FIELD )
    public UUID getId() {
        return id;
    }

    @JsonProperty( SerializationConstants.NAME )
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

    @JsonProperty( SerializationConstants.ENTITY_TYPE_ID )
    public UUID getEntityTypeId() {
        return entityTypeId;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        CollectionTemplateType that = (CollectionTemplateType) o;
        return Objects.equals( id, that.id ) &&
                Objects.equals( name, that.name ) &&
                Objects.equals( title, that.title ) &&
                Objects.equals( description, that.description ) &&
                Objects.equals( entityTypeId, that.entityTypeId );
    }

    @Override
    public int hashCode() {
        return Objects.hash( id, name, title, description, entityTypeId );
    }

    @Override
    public String toString() {
        return "CollectionTemplateType{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", title='" + title + '\'' +
                ", description='" + description + '\'' +
                ", entityTypeId=" + entityTypeId +
                '}';
    }
}
