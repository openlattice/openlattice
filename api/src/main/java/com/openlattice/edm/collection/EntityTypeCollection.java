package com.openlattice.edm.collection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.authorization.securable.AbstractSchemaAssociatedSecurableType;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.client.serialization.SerializationConstants;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

public class EntityTypeCollection extends AbstractSchemaAssociatedSecurableType {

    private LinkedHashSet<CollectionTemplateType> template;

    @JsonCreator
    public EntityTypeCollection(
            @JsonProperty( SerializationConstants.ID_FIELD ) Optional<UUID> id,
            @JsonProperty( SerializationConstants.TYPE_FIELD ) FullQualifiedName type,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.SCHEMAS ) Set<FullQualifiedName> schemas,
            @JsonProperty( SerializationConstants.TEMPLATE ) LinkedHashSet<CollectionTemplateType> template ) {
        super( id, type, title, description, schemas );

        /** Validate template objects **/
        Set<UUID> idsSeen = new HashSet<>( template.size() );
        Set<String> namesSeen = new HashSet<>( template.size() );
        template.forEach( collectionTemplateType -> {
            checkArgument( !idsSeen.contains( collectionTemplateType.getId() ),
                    "EntityTypeCollection template type ids must be distinct." );
            checkArgument( !namesSeen.contains( collectionTemplateType.getName() ),
                    "EntityTypeCollection template type names must be distinct." );

            idsSeen.add( collectionTemplateType.getId() );
            namesSeen.add( collectionTemplateType.getName() );
        } );

        this.template = template;
    }

    public EntityTypeCollection(
            UUID id,
            FullQualifiedName type,
            String title,
            Optional<String> description,
            Set<FullQualifiedName> schemas,
            LinkedHashSet<CollectionTemplateType> template ) {
        this( Optional.of( id ), type, title, description, schemas, template );
    }

    @JsonProperty( SerializationConstants.TEMPLATE )
    public LinkedHashSet<CollectionTemplateType> getTemplate() {
        return template;
    }

    public void addTypeToTemplate( CollectionTemplateType type ) {
        template.add( type );
    }

    public void removeTemplateTypeFromTemplate( UUID id ) {
        template.removeIf( type -> type.getId().equals( id ) );
    }

    public void removeTemplateTypeFromTemplate( String name ) {
        template.removeIf( type -> type.getName().equals( name ) );
    }

    @Override
    @JsonIgnore
    public SecurableObjectType getCategory() {
        return SecurableObjectType.EntityTypeCollection;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        if ( !super.equals( o ) )
            return false;
        EntityTypeCollection that = (EntityTypeCollection) o;
        return Objects.equals( template, that.template );
    }

    @Override public int hashCode() {
        return Objects.hash( super.hashCode(), template );
    }

    @Override public String toString() {
        return "EntityTypeCollection{" +
                "template=" + template +
                ", schemas=" + schemas +
                ", type=" + type +
                ", id=" + id +
                ", title='" + title + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
