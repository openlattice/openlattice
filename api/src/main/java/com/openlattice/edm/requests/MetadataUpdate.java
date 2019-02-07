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

import com.google.common.collect.LinkedHashMultimap;
import com.openlattice.authorization.Principal;
import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.Set;

/**
 * Used for updating metadata of property type, entity type, or entity set. Non-existent fields for the specific object
 * would be ignored.
 */
public class MetadataUpdate {

    // Common across property type, entity type, entity set
    private Optional<String>                    title;
    private Optional<String>                    description;
    // Specific to entity set
    private Optional<String>                           name;
    private Optional<Set<String>>                      contacts;
    // Specific to property type/entity type
    private Optional<FullQualifiedName>                type;
    // Specific to property type
    private Optional<Boolean>                          pii;
    // Specific to entity set property type metadata
    private Optional<Boolean>                          defaultShow;
    private Optional<String>                           url;
    private Optional<LinkedHashMultimap<UUID, String>> propertyTags;
    private Optional<UUID>                             organizationId;

        @JsonCreator
    public MetadataUpdate(
            @JsonProperty( SerializationConstants.TITLE_FIELD ) Optional<String> title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.NAME_FIELD ) Optional<String> name,
            @JsonProperty( SerializationConstants.CONTACTS ) Optional<Set<String>> contacts,
            @JsonProperty( SerializationConstants.TYPE_FIELD ) Optional<FullQualifiedName> type,
            @JsonProperty( SerializationConstants.PII_FIELD ) Optional<Boolean> pii,
            @JsonProperty( SerializationConstants.DEFAULT_SHOW ) Optional<Boolean> defaultShow,
            @JsonProperty( SerializationConstants.URL ) Optional<String> url,
            @JsonProperty( SerializationConstants.PROPERTY_TAGS ) Optional<LinkedHashMultimap<UUID, String>> propertyTags,
            @JsonProperty(SerializationConstants.ORGANIZATION) Optional<UUID> organizationId ) {
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
        this.propertyTags = propertyTags;
        this.organizationId = organizationId;
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

    @JsonProperty( SerializationConstants.PROPERTY_TAGS )
    public Optional<LinkedHashMultimap<UUID, String>> getPropertyTags() {
        return propertyTags;
    }

    @JsonProperty(SerializationConstants.ORGANIZATION)
    public Optional<UUID> getOrganizationId() {
        return organizationId;
    }

    @Override public String toString() {
        return "MetadataUpdate{" +
                "title=" + title +
                ", description=" + description +
                ", name=" + name +
                ", contacts=" + contacts +
                ", type=" + type +
                ", pii=" + pii +
                ", defaultShow=" + defaultShow +
                ", url=" + url +
                ", propertyTags=" + propertyTags +
                ", organization=" + organizationId +
                '}';
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof MetadataUpdate ) ) { return false; }
        MetadataUpdate that = (MetadataUpdate) o;
        return Objects.equals( title, that.title ) &&
                Objects.equals( description, that.description ) &&
                Objects.equals( name, that.name ) &&
                Objects.equals( contacts, that.contacts ) &&
                Objects.equals( type, that.type ) &&
                Objects.equals( pii, that.pii ) &&
                Objects.equals( defaultShow, that.defaultShow ) &&
                Objects.equals( url, that.url ) &&
                Objects.equals( propertyTags, that.propertyTags ) &&
                Objects.equals( organizationId, that.organizationId );
    }

    @Override public int hashCode() {
        return Objects
                .hash( title, description, name, contacts, type, pii, defaultShow, url, propertyTags, organizationId );
    }

    //TODO: Delete the code below as it doesn't seem to be used.
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
                Optional.empty(),
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
                Optional.empty(),
                Optional.empty(),
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
                update.getUrl(),
                Optional.empty(),
                Optional.empty() );
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
                Optional.empty(),
                Optional.empty(),
                Optional.empty() );
    }
}
