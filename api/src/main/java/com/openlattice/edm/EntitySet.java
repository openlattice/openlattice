/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

package com.openlattice.edm;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.openlattice.authorization.securable.AbstractSecurableObject;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntitySet extends AbstractSecurableObject {
    private final UUID        entityTypeId;
    private       String      name;
    private       Set<String> contacts;
    private final boolean     external;

    /**
     * Creates an entity set with provided parameters and will automatically generate a UUID if not provided.
     *
     * @param id An optional UUID for the entity set.
     * @param name The name of the entity set.
     * @param title The friendly name for the entity set.
     * @param description A description of the entity set.
     */
    @JsonCreator
    public EntitySet(
            @JsonProperty( SerializationConstants.ID_FIELD ) Optional<UUID> id,
            @JsonProperty( SerializationConstants.ENTITY_TYPE_ID_FIELD ) UUID entityTypeId,
            @JsonProperty( SerializationConstants.NAME_FIELD ) String name,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.CONTACTS ) Set<String> contacts,
            @JsonProperty( SerializationConstants.EXTERNAL ) Optional<Boolean> external ) {
        super( id, title, description );
        checkArgument( StringUtils.isNotBlank( name ), "Entity set name cannot be blank." );
        // Temporary
        //        checkArgument( contacts != null && !contacts.isEmpty(), "Contacts cannot be blank." );
        this.name = name;
        this.entityTypeId = checkNotNull( entityTypeId );
        this.contacts = Sets.newHashSet( contacts );
        this.external = external.or( true ); //Default to external
    }

    public EntitySet(
            UUID id,
            UUID entityTypeId,
            String name,
            String title,
            Optional<String> description,
            Set<String> contacts ) {
        this( Optional.of( id ), entityTypeId, name, title, description, contacts, Optional.of(true ));
    }

    public EntitySet(
            UUID entityTypeId,
            String name,
            String title,
            Optional<String> description,
            Set<String> contacts ) {
        this( Optional.absent(), entityTypeId, name, title, description, contacts, Optional.of( true )) ;
    }

    @JsonProperty( SerializationConstants.ENTITY_TYPE_ID_FIELD )
    public UUID getEntityTypeId() {
        return entityTypeId;
    }

    @JsonProperty( SerializationConstants.NAME_FIELD )
    public String getName() {
        return name;
    }

    public void setName( String name ) {
        this.name = name;
    }

    @JsonProperty( SerializationConstants.CONTACTS )
    public Set<String> getContacts() {
        return contacts;
    }

    public void setContacts( Set<String> contacts ) {
        this.contacts = contacts;
    }

    @JsonProperty(SerializationConstants.EXTERNAL)
    public boolean isExternal() {
        return external;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof EntitySet ) ) { return false; }
        if ( !super.equals( o ) ) { return false; }

        EntitySet entitySet = (EntitySet) o;

        if ( external != entitySet.external ) { return false; }
        if ( !entityTypeId.equals( entitySet.entityTypeId ) ) { return false; }
        if ( !name.equals( entitySet.name ) ) { return false; }
        return contacts.equals( entitySet.contacts );
    }

    @Override public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + entityTypeId.hashCode();
        result = 31 * result + ( external ? 1 : 0 );
        result = 31 * result + name.hashCode();
        result = 31 * result + contacts.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "EntitySet [entityTypeId=" + entityTypeId + ", name=" + name + ", contacts=" + contacts + ", id=" + id
                + ", title=" + title + ", description=" + description + "]";
    }

    @Override
    @JsonIgnore
    public SecurableObjectType getCategory() {
        return SecurableObjectType.EntitySet;
    }
}
