package com.openlattice.edm.collection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.authorization.securable.AbstractSecurableObject;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.organization.OrganizationConstants;

import java.util.*;

public class EntitySetCollection extends AbstractSecurableObject {

    private String            name;
    private UUID              entityTypeCollectionId;
    private Map<UUID, UUID> template;
    private Set<String>       contacts;
    private UUID              organizationId;

    @JsonCreator
    public EntitySetCollection(
            @JsonProperty( SerializationConstants.ID_FIELD ) Optional<UUID> id,
            @JsonProperty( SerializationConstants.NAME_FIELD ) String name,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.ENTITY_TYPE_COLLECTION_ID ) UUID entityTypeCollectionId,
            @JsonProperty( SerializationConstants.TEMPLATE ) Map<UUID, UUID> template,
            @JsonProperty( SerializationConstants.CONTACTS ) Set<String> contacts,
            @JsonProperty( SerializationConstants.ORGANIZATION_ID ) Optional<UUID> organizationId ) {
        super( id, title, description );
        this.name = name;
        this.entityTypeCollectionId = entityTypeCollectionId;
        this.template = template;
        this.contacts = contacts;
        this.organizationId = organizationId.orElse( OrganizationConstants.GLOBAL_ORGANIZATION_ID );
    }

    public EntitySetCollection(
            UUID id,
            String name,
            String title,
            Optional<String> description,
            UUID entityTypeCollectionId,
            Map<UUID, UUID> template,
            Set<String> contacts,
            Optional<UUID> organizationId ) {
        this( Optional.of( id ), name, title, description, entityTypeCollectionId, template, contacts, organizationId );
    }

    @JsonProperty( SerializationConstants.NAME_FIELD )
    public String getName() {
        return name;
    }

    @JsonProperty( SerializationConstants.ENTITY_TYPE_COLLECTION_ID )
    public UUID getEntityTypeCollectionId() {
        return entityTypeCollectionId;
    }

    @JsonProperty( SerializationConstants.TEMPLATE )
    public Map<UUID, UUID> getTemplate() {
        return template;
    }

    @JsonProperty( SerializationConstants.CONTACTS )
    public Set<String> getContacts() {
        return contacts;
    }

    @JsonProperty( SerializationConstants.ORGANIZATION_ID )
    public UUID getOrganizationId() {
        return organizationId;
    }

    public void setName( String name ) {
        this.name = name;
    }

    public void setContacts( Set<String> contacts ) {
        this.contacts = contacts;
    }

    public void setTemplate( Map<UUID, UUID> template ) {
        this.template = template;
    }

    public void setOrganizationId( UUID organizationId ) {
        this.organizationId = organizationId;
    }

    @JsonIgnore
    @Override
    public SecurableObjectType getCategory() {
        return SecurableObjectType.EntitySetCollection;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        if ( !super.equals( o ) )
            return false;
        EntitySetCollection that = (EntitySetCollection) o;
        return Objects.equals( name, that.name ) &&
                Objects.equals( entityTypeCollectionId, that.entityTypeCollectionId ) &&
                Objects.equals( template, that.template ) &&
                Objects.equals( contacts, that.contacts ) &&
                Objects.equals( organizationId, that.organizationId );
    }

    @Override
    public int hashCode() {
        return Objects.hash( super.hashCode(), name, entityTypeCollectionId, template, contacts, organizationId );
    }

    @Override
    public String toString() {
        return "EntitySetCollection{" +
                "name='" + name + '\'' +
                ", entityTypeCollectionId=" + entityTypeCollectionId +
                ", template=" + template +
                ", contacts=" + contacts +
                ", organizationId=" + organizationId +
                ", id=" + id +
                ", title='" + title + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
