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

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.graph.query.GraphQueryState.Option;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

/**
 * An entity type describes the set of property collections.
 */
public class EntityType extends ComplexType {
    private final     LinkedHashSet<UUID> key;
    private final     Optional<UUID>      baseType;
    private transient int                 h = 0;

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
            @JsonProperty( SerializationConstants.CATEGORY ) Optional<SecurableObjectType> category ) {
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
            Optional<SecurableObjectType> category ) {
        this( Optional.of( id ),
                type,
                title,
                description,
                schemas,
                key,
                properties,
                Optional.of( propertyTags ),
                baseType,
                category );
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
            Optional<SecurableObjectType> category ) {
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
                category );
    }

    @Override public String toString() {
        return "EntityType{" +
                "key=" + key +
                ", baseType=" + baseType +
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

    public void addPrimaryKeys( Set<UUID> propertyTypeIds ) {
        key.addAll( checkNotNull( propertyTypeIds, "Property type ids cannot be null." ) );
    }

    public void removePrimaryKeys( Set<UUID> propertyTypeIds ) {
        key.removeAll( checkNotNull( propertyTypeIds, "Property type ids cannot be null." ) );
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( o == null || getClass() != o.getClass() ) { return false; }
        if ( !super.equals( o ) ) { return false; }

        EntityType that = (EntityType) o;

        if ( !key.equals( that.key ) ) { return false; }
        return baseType.equals( that.baseType );
    }

    @Override
    public int hashCode() {
        if ( h == 0 ) {
            int result = super.hashCode();
            result = 31 * result + key.hashCode();
            result = 31 * result + baseType.hashCode();
            h = result;
        }
        return h;
    }

}
