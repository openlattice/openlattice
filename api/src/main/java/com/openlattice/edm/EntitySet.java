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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;
import com.openlattice.authorization.securable.AbstractSecurableObject;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.client.serialization.SerializationConstants;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;

/**
 * Describes an entity set and associated metadata, including the active audit record entity set.
 *
 * TODO: Ensure that we are being consistent around how internal sets are accessed and modified. i.e is it okay
 * to return modifiable versions of linked entity sets or should we expose add/remove/update methods. The latter seems
 * like the most explicitly safe thing to do.
 */
public class EntitySet extends AbstractSecurableObject {
    private final UUID        entityTypeId;
    private final boolean     linking;
    private final Set<UUID>   linkedEntitySets;
    private final boolean     external;
    private       String      name;
    private       Set<String> contacts;

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
            @JsonProperty( SerializationConstants.ENTITY_TYPE_ID ) UUID entityTypeId,
            @JsonProperty( SerializationConstants.NAME_FIELD ) String name,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.CONTACTS ) Set<String> contacts,
            @JsonProperty( SerializationConstants.LINKING ) Optional<Boolean> linking,
            @JsonProperty( SerializationConstants.LINKED_ENTITY_SETS ) Optional<Set<UUID>> linkedEntitySets,
            @JsonProperty( SerializationConstants.EXTERNAL ) Optional<Boolean> external ) {
        super( id, title, description );
        this.linking = linking.orElse( false );
        this.linkedEntitySets = linkedEntitySets.orElse( new HashSet<>() );
        checkArgument( StringUtils.isNotBlank( name ), "Entity set name cannot be blank." );
        checkArgument( this.linkedEntitySets.isEmpty() || this.linking,
                "You cannot specify linked entity sets unless this is a linking entity set." );

        // Temporary
        //        checkArgument( contacts != null && !contacts.isEmpty(), "Contacts cannot be blank." );
        this.name = name;
        this.entityTypeId = checkNotNull( entityTypeId );
        this.contacts = Sets.newHashSet( contacts );
        this.external = external.orElse( true ); //Default to external
    }

    public EntitySet(
            UUID id,
            UUID entityTypeId,
            String name,
            String title,
            Optional<String> description,
            Set<String> contacts ) {
        this( Optional.of( id ),
                entityTypeId,
                name,
                title,
                description,
                contacts,
                Optional.empty(),
                Optional.empty(),
                Optional.of( true ) );
    }

    public EntitySet(
            UUID entityTypeId,
            String name,
            String title,
            Optional<String> description,
            Set<String> contacts ) {
        this( Optional.empty(),
                entityTypeId,
                name,
                title,
                description,
                contacts,
                Optional.empty(),
                Optional.empty(),
                Optional.of( true ) );
    }

    @JsonProperty( SerializationConstants.ENTITY_TYPE_ID )
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

    @JsonProperty( SerializationConstants.EXTERNAL )
    public boolean isExternal() {
        return external;
    }

    @JsonProperty( SerializationConstants.LINKING )
    public boolean isLinking() {
        return linking;
    }

    @JsonProperty( SerializationConstants.LINKED_ENTITY_SETS )
    public Set<UUID> getLinkedEntitySets() {
        return linkedEntitySets;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof EntitySet ) ) { return false; }
        if ( !super.equals( o ) ) { return false; }
        EntitySet entitySet = (EntitySet) o;
        return linking == entitySet.linking &&
                external == entitySet.external &&
                Objects.equals( entityTypeId, entitySet.entityTypeId ) &&
                Objects.equals( linkedEntitySets, entitySet.linkedEntitySets ) &&
                Objects.equals( name, entitySet.name ) &&
                Objects.equals( contacts, entitySet.contacts );
    }

    @Override public int hashCode() {

        return Objects.hash( super.hashCode(), entityTypeId, linking, linkedEntitySets, external, name, contacts );
    }

    @Override public String toString() {
        return "EntitySet{" +
                "entityTypeId=" + entityTypeId +
                ", linking=" + linking +
                ", linkedEntitySets=" + linkedEntitySets +
                ", external=" + external +
                ", name='" + name + '\'' +
                ", contacts=" + contacts +
                ", id=" + id +
                ", title='" + title + '\'' +
                ", description='" + description + '\'' +
                '}';
    }

    @Override
    @JsonIgnore
    public SecurableObjectType getCategory() {
        return SecurableObjectType.EntitySet;
    }
}
