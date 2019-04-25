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

package com.openlattice.edm.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedHashMultimap;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.client.serialization.SerializationConstants;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An entity type describes the set of property collections.
 */
public class EntityType extends ComplexType {
    private static final int DEFAULT_SHARDS = 5;
    private static final int MAX_SHARDS     = 20;

    private final     LinkedHashSet<UUID> key;
    private final     Optional<UUID>      baseType;
    private transient int                 h = 0;
    private final     int                 shards;

    @JsonCreator
    public EntityType(
            @JsonProperty( SerializationConstants.ID_FIELD ) Optional<UUID> id,
            @JsonProperty( SerializationConstants.TYPE_FIELD ) FullQualifiedName type,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.SCHEMAS ) Set<FullQualifiedName> schemas,
            @JsonProperty( SerializationConstants.KEY_FIELD ) LinkedHashSet<UUID> key,
            @JsonProperty( SerializationConstants.PROPERTIES_FIELD ) LinkedHashSet<UUID> properties,
            @JsonProperty( SerializationConstants.PROPERTY_TAGS )
                    Optional<LinkedHashMultimap<UUID, String>> propertyTags,
            @JsonProperty( SerializationConstants.BASE_TYPE_FIELD ) Optional<UUID> baseType,
            @JsonProperty( SerializationConstants.CATEGORY ) Optional<SecurableObjectType> category,
            @JsonProperty( SerializationConstants.SHARDS ) Optional<Integer> shards ) {
        super(
                id,
                type,
                title,
                description,
                schemas,
                properties,
                propertyTags,
                baseType,
                category.orElse( SecurableObjectType.EntityType ) );
        this.key = Preconditions.checkNotNull( key, "Entity set key properties cannot be null" );
        Preconditions.checkArgument( !key.isEmpty() || baseType.isPresent(), "Key properties cannot be empty" );
        Preconditions.checkNotNull( properties, "Entity set properties cannot be null" );
        Preconditions
                .checkArgument( properties.containsAll( key ) || baseType.isPresent(),
                        "Properties must include all the key property types" );
        this.shards = shards.orElse( DEFAULT_SHARDS );
        Preconditions.checkArgument( this.shards > 0 && this.shards < MAX_SHARDS,
                "The number of shards must be between 0 and " + MAX_SHARDS );
        this.baseType = baseType;
    }

    public EntityType(
            UUID id,
            FullQualifiedName type,
            String title,
            Optional<String> description,
            Set<FullQualifiedName> schemas,
            LinkedHashSet<UUID> key,
            LinkedHashSet<UUID> properties,
            LinkedHashMultimap<UUID, String> propertyTags,
            Optional<UUID> baseType,
            Optional<SecurableObjectType> category,
            Optional<Integer> shards ) {
        this( Optional.of( id ),
                type,
                title,
                description,
                schemas,
                key,
                properties,
                Optional.of( propertyTags ),
                baseType,
                category,
                shards );
    }

    public EntityType(
            FullQualifiedName type,
            String title,
            String description,
            Set<FullQualifiedName> schemas,
            LinkedHashSet<UUID> key,
            LinkedHashSet<UUID> properties,
            LinkedHashMultimap<UUID, String> propertyTags,
            Optional<UUID> baseType,
            Optional<SecurableObjectType> category,
            Optional<Integer> shards ) {
        this(
                Optional.empty(),
                type,
                title,
                Optional.of( description ),
                schemas,
                key,
                properties,
                Optional.empty(),
                baseType,
                category,
                shards );
    }

    @Override public String toString() {
        return "EntityType{" +
                "key=" + key +
                ", baseType=" + baseType +
                ", h=" + h +
                ", shards=" + shards +
                ", propertyTags=" + propertyTags +
                ", properties=" + properties +
                ", schemas=" + schemas +
                ", type=" + type +
                ", id=" + id +
                ", title='" + title + '\'' +
                ", description='" + description + '\'' +
                '}';
    }

    // TODO: It seems the objects do not allow property types from the different schemas.
    @JsonProperty( SerializationConstants.KEY_FIELD )
    public Set<UUID> getKey() {
        return Collections.unmodifiableSet( key );
    }

    @JsonProperty( SerializationConstants.BASE_TYPE_FIELD )
    public Optional<UUID> getBaseType() {
        return baseType;
    }

    @JsonProperty( SerializationConstants.SHARDS )
    public int getShards() {
        return shards;
    }

    public void addPrimaryKeys( Set<UUID> propertyTypeIds ) {
        key.addAll( checkNotNull( propertyTypeIds, "Property type ids cannot be null." ) );
    }

    public void removePrimaryKeys( Set<UUID> propertyTypeIds ) {
        key.removeAll( checkNotNull( propertyTypeIds, "Property type ids cannot be null." ) );
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        if ( !super.equals( o ) )
            return false;
        EntityType that = (EntityType) o;
        return h == that.h &&
                shards == that.shards &&
                Objects.equals( key, that.key ) &&
                Objects.equals( baseType, that.baseType );
    }

    @Override public int hashCode() {
        return Objects.hash( super.hashCode(), key, baseType, h, shards );
    }
}
