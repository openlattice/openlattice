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
import com.openlattice.data.DataExpiration;
import com.openlattice.edm.set.EntitySetFlag;
import com.openlattice.entitysets.StorageType;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;

/**
 * Describes an entity set and associated metadata, including the active audit record entity set.
 * <p>
 * TODO: Ensure that we are being consistent around how internal sets are accessed and modified. i.e is it okay
 * to return modifiable versions of linked entity sets or should we expose add/remove/update methods. The latter seems
 * like the most explicitly safe thing to do.
 */
public class EntitySetOld extends AbstractSecurableObject {
    private final UUID                   entityTypeId;
    private final Set<UUID>              linkedEntitySetOlds;
    private final EnumSet<EntitySetFlag> flags;
    private final Set<Integer>           partitions        = new LinkedHashSet<>( 8 );
    private       String                 name;
    private       Set<String>            contacts;
    private       UUID                   organizationId;
    private       int                    partitionsVersion = 0;
    private       DataExpiration         expiration;
    private       StorageType            storageType;

    /**
     * Creates an entity set with provided parameters and will automatically generate a UUID if not provided.
     *
     * @param id An optional UUID for the entity set.
     * @param name The name of the entity set.
     * @param title The friendly name for the entity set.
     * @param description A description of the entity set.
     */
    @JsonCreator
    public EntitySetOld(
            @JsonProperty( SerializationConstants.ID_FIELD ) Optional<UUID> id,
            @JsonProperty( SerializationConstants.ENTITY_TYPE_ID ) UUID entityTypeId,
            @JsonProperty( SerializationConstants.NAME_FIELD ) String name,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.CONTACTS ) Set<String> contacts,
            @JsonProperty( SerializationConstants.LINKED_ENTITY_SETS ) Optional<Set<UUID>> linkedEntitySetOlds,
            @JsonProperty( SerializationConstants.ORGANIZATION_ID ) UUID organizationId,
            @JsonProperty( SerializationConstants.FLAGS_FIELD ) Optional<EnumSet<EntitySetFlag>> flags,
            @JsonProperty( SerializationConstants.PARTITIONS ) Optional<LinkedHashSet<Integer>> partitions,
            @JsonProperty( SerializationConstants.EXPIRATION ) Optional<DataExpiration> expiration,
            @JsonProperty( SerializationConstants.STORAGE_TYPE ) Optional<StorageType> storageType) {
        super( id, title, description );
        this.linkedEntitySetOlds = linkedEntitySetOlds.orElse( new HashSet<>() );
        this.flags = flags.orElse( EnumSet.of( EntitySetFlag.EXTERNAL ) );
        checkArgument( StringUtils.isNotBlank( name ), "Entity set name cannot be blank." );
        checkArgument( this.linkedEntitySetOlds.isEmpty() || isLinking(),
                "You cannot specify linked entity sets unless this is a linking entity set." );

        // Temporary
        //        checkArgument( contacts != null && !contacts.isEmpty(), "Contacts cannot be blank." );
        this.name = name;
        this.entityTypeId = checkNotNull( entityTypeId );
        if ( contacts instanceof HashSet ) {
            this.contacts = contacts;
        } else {
            this.contacts = Sets.newHashSet( contacts );
        }
        this.organizationId = checkNotNull( organizationId );
        partitions.ifPresent( this.partitions::addAll );
        this.expiration = expiration.orElse( null );
        this.storageType = storageType.orElse( StorageType.STANDARD );
    }

    //Constructor for serialization
    public EntitySetOld(
            UUID id,
            UUID entityTypeId,
            String name,
            String title,
            String description,
            Set<String> contacts,
            Set<UUID> linkedEntitySetOlds,
            UUID organizationId,
            EnumSet<EntitySetFlag> flags,
            LinkedHashSet<Integer> partitions,
            DataExpiration expiration
    ) {
        super( id, title, description );
        this.linkedEntitySetOlds = linkedEntitySetOlds;
        this.flags = flags;
        checkArgument( StringUtils.isNotBlank( name ), "Entity set name cannot be blank." );
        checkArgument( this.linkedEntitySetOlds.isEmpty() || isLinking(),
                "You cannot specify linked entity sets unless this is a linking entity set." );

        // Temporary
        //        checkArgument( contacts != null && !contacts.isEmpty(), "Contacts cannot be blank." );
        this.name = name;
        this.entityTypeId = checkNotNull( entityTypeId );
        if ( contacts instanceof HashSet ) {
            this.contacts = contacts;
        } else {
            this.contacts = Sets.newHashSet( contacts );
        }
        this.organizationId = organizationId;
        this.partitions.addAll( partitions );
        this.expiration = expiration;
    }

    public EntitySetOld(
            UUID id,
            UUID entityTypeId,
            String name,
            String title,
            Optional<String> description,
            Set<String> contacts,
            UUID organizationId ) {
        this( Optional.of( id ),
                entityTypeId,
                name,
                title,
                description,
                contacts,
                Optional.empty(),
                organizationId,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public EntitySetOld(
            UUID entityTypeId,
            String name,
            String title,
            Optional<String> description,
            Set<String> contacts,
            Optional<Set<UUID>> linkedEntitySetOlds,
            UUID organizationId,
            Optional<EnumSet<EntitySetFlag>> flags,
            Optional<LinkedHashSet<Integer>> partitions ) {
        this( Optional.empty(),
                entityTypeId,
                name,
                title,
                description,
                contacts,
                linkedEntitySetOlds,
                organizationId,
                flags,
                partitions,
                Optional.empty(),
                Optional.empty());
    }

    @JsonProperty( SerializationConstants.ORGANIZATION_ID )
    public UUID getOrganizationId() {
        return organizationId;
    }

    public void setOrganizationId( UUID organizationId ) {
        this.organizationId = organizationId;
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

    @JsonProperty( SerializationConstants.LINKED_ENTITY_SETS )
    public Set<UUID> getLinkedEntitySetOlds() {
        return linkedEntitySetOlds;
    }

    @JsonProperty( SerializationConstants.FLAGS_FIELD )
    public EnumSet<EntitySetFlag> getFlags() {
        return flags;
    }

    @JsonProperty( SerializationConstants.PARTITIONS )
    public Set<Integer> getPartitions() {
        return partitions;
    }

    @JsonIgnore
    public void setPartitions( Collection<Integer> partitions ) {
        this.partitions.clear();
        addPartitions( partitions );
    }

    @JsonProperty( SerializationConstants.STORAGE_TYPE )
    public StorageType getStorageType() {
        return storageType;
    }

    @JsonProperty
    public DataExpiration getExpiration() {
        return expiration;
    }

    public void setExpiration( DataExpiration expiration ) {
        this.expiration = expiration;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( o == null || getClass() != o.getClass() ) { return false; }
        if ( !super.equals( o ) ) { return false; }
        EntitySetOld EntitySetOld = (EntitySetOld) o;
        return Objects.equals( entityTypeId, EntitySetOld.entityTypeId ) &&
                Objects.equals( linkedEntitySetOlds, EntitySetOld.linkedEntitySetOlds ) &&
                Objects.equals( flags, EntitySetOld.flags ) &&
                Objects.equals( partitions, EntitySetOld.partitions ) &&
                Objects.equals( name, EntitySetOld.name ) &&
                Objects.equals( contacts, EntitySetOld.contacts ) &&
                Objects.equals( organizationId, EntitySetOld.organizationId ) &&
                Objects.equals( expiration, EntitySetOld.expiration ) &&
                Objects.equals( storageType, EntitySetOld.storageType );
    }

    @Override
    public int hashCode() {
        return Objects.hash( super.hashCode(),
                entityTypeId,
                linkedEntitySetOlds,
                flags,
                partitions,
                name,
                contacts,
                organizationId,
                expiration,
                storageType );
    }

    @JsonIgnore
    public int getPartitionsVersion() {
        return partitionsVersion;
    }

    public void addFlag( EntitySetFlag flag ) {
        this.flags.add( flag );
    }

    public void removeFlag( EntitySetFlag flag ) {
        this.flags.remove( flag );
    }

    void addPartitions( Collection<Integer> partitions ) {
        this.partitions.addAll( partitions );
    }

    @JsonIgnore
    public boolean isExternal() {
        return flags.contains( EntitySetFlag.EXTERNAL );
    }

    @JsonIgnore
    public boolean isLinking() {
        return flags.contains( EntitySetFlag.LINKING );
    }

    @JsonIgnore
    public boolean hasExpirationPolicy() {
        return expiration != null;
    }

    @Override
    @JsonIgnore
    public SecurableObjectType getCategory() {
        return SecurableObjectType.EntitySet;
    }
}
