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

package com.openlattice.edm.requests;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.Set;

/**
 * Used for updating metadata of property type, entity type, or entity set. Non-existent fields for the specific object
 * would be ignored.
 *
 * @author Ho Chung Siu
 */
public class MetadataUpdate {

    // Common across property type, entity type, entity set
    private Optional<String>            title;
    private Optional<String>            description;
    // Specific to entity set
    private Optional<String>            name;
    private Optional<Set<String>>       contacts;
    // Specific to property type/entity type
    private Optional<FullQualifiedName> type;
    // Specific to property type
    private Optional<Boolean>           pii;
    // Specific to entity set property type metadata
    private Optional<Boolean>           defaultShow;
    private Optional<String>            url;

    @JsonCreator
    public MetadataUpdate(
            @JsonProperty( SerializationConstants.TITLE_FIELD ) Optional<String> title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.NAME_FIELD ) Optional<String> name,
            @JsonProperty( SerializationConstants.CONTACTS ) Optional<Set<String>> contacts,
            @JsonProperty( SerializationConstants.TYPE_FIELD ) Optional<FullQualifiedName> type,
            @JsonProperty( SerializationConstants.PII_FIELD ) Optional<Boolean> pii,
            @JsonProperty( SerializationConstants.DEFAULT_SHOW ) Optional<Boolean> defaultShow,
            @JsonProperty( SerializationConstants.URL ) Optional<String> url ) {
        // WARNING These checks have to be consistent with the same check elsewhere.
        Preconditions.checkArgument( !title.isPresent() || StringUtils.isNotBlank( title.get() ),
                "Title cannot be blank." );
        Preconditions.checkArgument( !name.isPresent() || StringUtils.isNotBlank( name.get() ),
                "Entity set name cannot be blank." );
        Preconditions.checkArgument( !contacts.isPresent() || !contacts.get().isEmpty(), "Contacts cannot be blank." );
        Preconditions.checkArgument( !type.isPresent() || StringUtils.isNotBlank( type.get().getNamespace() ),
                "Namespace of type is missing." );
        Preconditions.checkArgument( !type.isPresent() || StringUtils.isNotBlank( type.get().getName() ),
                "Name of type is missing." );

        this.title = title;
        this.description = description;
        this.name = name;
        this.contacts = contacts;
        this.type = type;
        this.pii = pii;
        this.defaultShow = defaultShow;
        this.url = url;
    }

    @JsonProperty( SerializationConstants.TITLE_FIELD )
    public Optional<String> getTitle() {
        return title;
    }

    @JsonProperty( SerializationConstants.DESCRIPTION_FIELD )
    public Optional<String> getDescription() {
        return description;
    }

    @JsonProperty( SerializationConstants.NAME_FIELD )
    public Optional<String> getName() {
        return name;
    }

    @JsonProperty( SerializationConstants.CONTACTS )
    public Optional<Set<String>> getContacts() {
        return contacts;
    }

    @JsonProperty( SerializationConstants.TYPE_FIELD )
    public Optional<FullQualifiedName> getType() {
        return type;
    }

    @JsonProperty( SerializationConstants.PII_FIELD )
    public Optional<Boolean> getPii() {
        return pii;
    }

    @JsonProperty( SerializationConstants.DEFAULT_SHOW )
    public Optional<Boolean> getDefaultShow() {
        return defaultShow;
    }

    @JsonProperty( SerializationConstants.URL )
    public Optional<String> getUrl() {
        return url;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( contacts == null ) ? 0 : contacts.hashCode() );
        result = prime * result + ( ( defaultShow == null ) ? 0 : defaultShow.hashCode() );
        result = prime * result + ( ( description == null ) ? 0 : description.hashCode() );
        result = prime * result + ( ( name == null ) ? 0 : name.hashCode() );
        result = prime * result + ( ( pii == null ) ? 0 : pii.hashCode() );
        result = prime * result + ( ( title == null ) ? 0 : title.hashCode() );
        result = prime * result + ( ( type == null ) ? 0 : type.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj )
            return true;
        if ( obj == null )
            return false;
        if ( getClass() != obj.getClass() )
            return false;
        MetadataUpdate other = (MetadataUpdate) obj;
        if ( contacts == null ) {
            if ( other.contacts != null )
                return false;
        } else if ( !contacts.equals( other.contacts ) )
            return false;
        if ( defaultShow == null ) {
            if ( other.defaultShow != null )
                return false;
        } else if ( !defaultShow.equals( other.defaultShow ) )
            return false;
        if ( description == null ) {
            if ( other.description != null )
                return false;
        } else if ( !description.equals( other.description ) )
            return false;
        if ( name == null ) {
            if ( other.name != null )
                return false;
        } else if ( !name.equals( other.name ) )
            return false;
        if ( pii == null ) {
            if ( other.pii != null )
                return false;
        } else if ( !pii.equals( other.pii ) )
            return false;
        if ( title == null ) {
            if ( other.title != null )
                return false;
        } else if ( !title.equals( other.title ) )
            return false;
        if ( type == null ) {
            if ( other.type != null )
                return false;
        } else if ( !type.equals( other.type ) )
            return false;
        return true;
    }

    // Trimming happens before initializing update processors so that irrelevant fields won't get ser/deserialized when
    // processors are serialized.
    public static MetadataUpdate trimToPropertyTypeUpdate( MetadataUpdate update ) {
        return new MetadataUpdate(
                update.getTitle(),
                update.getDescription(),
                Optional.empty(),
                Optional.empty(),
                update.getType(),
                update.getPii(),
                Optional.empty(),
                Optional.empty() );
    }

    public static MetadataUpdate trimToEntityTypeUpdate( MetadataUpdate update ) {
        return new MetadataUpdate(
                update.getTitle(),
                update.getDescription(),
                Optional.empty(),
                Optional.empty(),
                update.getType(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty() );
    }

    public static MetadataUpdate trimToEntitySetUpdate( MetadataUpdate update ) {
        return new MetadataUpdate(
                update.getTitle(),
                update.getDescription(),
                update.getName(),
                update.getContacts(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty() );
    }

    public static MetadataUpdate trimToEntitySetPropertyMetadataUpdate( MetadataUpdate update ) {
        return new MetadataUpdate(
                update.getTitle(),
                update.getDescription(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                update.getDefaultShow(),
                Optional.empty() );
    }

    public static MetadataUpdate trimToAppUpdate( MetadataUpdate update ) {
        return new MetadataUpdate(
                update.getTitle(),
                update.getDescription(),
                update.getName(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                update.getUrl() );
    }

    public static MetadataUpdate trimToAppTypeUpdate( MetadataUpdate update ) {
        return new MetadataUpdate(
                update.getTitle(),
                update.getDescription(),
                Optional.empty(),
                Optional.empty(),
                update.getType(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty() );
    }
}
