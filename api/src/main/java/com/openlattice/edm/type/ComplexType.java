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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;
import com.openlattice.authorization.securable.AbstractSchemaAssociatedSecurableType;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.client.serialization.SerializationConstants;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

public class ComplexType extends AbstractSchemaAssociatedSecurableType {

    private final Optional<UUID> baseType;

    private final     SecurableObjectType              category;
    protected         LinkedHashMultimap<UUID, String> propertyTags;
    protected           LinkedHashSet<UUID>              properties;
    private transient int                              h = 0;

    @JsonCreator
    public ComplexType(
            @JsonProperty( SerializationConstants.ID_FIELD ) Optional<UUID> id,
            @JsonProperty( SerializationConstants.TYPE_FIELD ) FullQualifiedName type,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.SCHEMAS ) Set<FullQualifiedName> schemas,
            @JsonProperty( SerializationConstants.PROPERTIES_FIELD ) LinkedHashSet<UUID> properties,
            @JsonProperty( SerializationConstants.PROPERTY_TAGS )
                    Optional<LinkedHashMultimap<UUID, String>> propertyTags,
            @JsonProperty( SerializationConstants.PARENT_TYPE_FIELD ) Optional<UUID> baseType,
            @JsonProperty( SerializationConstants.CATEGORY ) SecurableObjectType category ) {
        super( id, type, title, description, schemas );
        this.properties = checkNotNull( properties, "Entity set properties cannot be null" );
        this.baseType = baseType;
        this.category = category;
        this.propertyTags = propertyTags.orElse( LinkedHashMultimap.create() );
    }

    public ComplexType(
            UUID id,
            FullQualifiedName type,
            String title,
            Optional<String> description,
            Set<FullQualifiedName> schemas,
            LinkedHashSet<UUID> properties,
            LinkedHashMultimap<UUID, String> propertyTags,
            Optional<UUID> baseType,
            SecurableObjectType category ) {
        this( Optional.of( id ),
                type,
                title,
                description,
                schemas,
                properties,
                Optional.of( propertyTags ),
                baseType,
                category );
    }

    public ComplexType(
            FullQualifiedName type,
            String title,
            String description,
            Set<FullQualifiedName> schemas,
            LinkedHashSet<UUID> properties,
            LinkedHashMultimap<UUID, String> propertyTags,
            Optional<UUID> baseType,
            SecurableObjectType category
    ) {
        this( Optional.empty(),
                type,
                title,
                Optional.of( description ),
                schemas,
                properties,
                Optional.of( propertyTags ),
                baseType,
                category );
    }

    @Override public String toString() {
        return "ComplexType{" +
                "baseType=" + baseType +
                ", category=" + category +
                ", propertyTags=" + propertyTags +
                ", properties=" + properties +
                ", schemas=" + schemas +
                ", type=" + type +
                ", id=" + id +
                ", title='" + title + '\'' +
                ", description='" + description + '\'' +
                '}';
    }

    @JsonProperty( SerializationConstants.PARENT_TYPE_FIELD )
    public Optional<UUID> getBaseType() {
        return baseType;
    }

    @JsonProperty( SerializationConstants.PROPERTIES_FIELD )
    public Set<UUID> getProperties() {
        return Collections.unmodifiableSet( properties );
    }

    @JsonProperty( SerializationConstants.PROPERTY_TAGS )
    public LinkedHashMultimap<UUID, String> getPropertyTags() {
        return propertyTags;
    }

    public void addPropertyTypes( Set<UUID> propertyTypeIds ) {
        properties.addAll( checkNotNull( propertyTypeIds, "Property type ids cannot be null." ) );
    }

    public void removePropertyTypes( Set<UUID> propertyTypeIds ) {
        properties.removeAll( checkNotNull( propertyTypeIds, "Property type ids cannot be null." ) );
    }

    public void reorderPropertyTypes( LinkedHashSet<UUID> propertyTypeIds ) {
        checkArgument( properties.equals( propertyTypeIds ),
                "The property types in the reordered list do not match the entity type's property types." );
        properties = propertyTypeIds;
    }

    public void setPropertyTypeTags( LinkedHashMultimap<UUID, String> propertyTags ) {
        this.propertyTags = propertyTags;
    }

    @Override
    @JsonProperty( SerializationConstants.CATEGORY )
    public SecurableObjectType getCategory() {
        return category;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof ComplexType ) ) { return false; }
        if ( !super.equals( o ) ) { return false; }
        ComplexType that = (ComplexType) o;
        return Objects.equals( baseType, that.baseType ) &&
                category == that.category &&
                Objects.equals( propertyTags, that.propertyTags ) &&
                Objects.equals( properties, that.properties );
    }

    @Override public int hashCode() {
        if ( h == 0 ) {
            h = Objects.hash( super.hashCode(), baseType, category, propertyTags, properties );
        }
        return h;
    }
}
