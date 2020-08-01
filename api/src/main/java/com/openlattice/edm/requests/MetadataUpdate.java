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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.data.DataExpiration;
import com.openlattice.postgres.IndexType;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Used for updating metadata of property type, entity type, or entity set. Non-existent fields for the specific object
 * would be ignored.
 */
public class MetadataUpdate {

    // Common across property type, entity type, entity set
    private Optional<String>                                     title;
    private Optional<String>                                     description;
    private Optional<IndexType>                                  indexType;
    // Specific to entity set
    private Optional<String>                                     name;
    private Optional<Set<String>>                                contacts;
    private Optional<DataExpiration>                             dataExpiration;
    // Specific to property type/entity type
    private Optional<FullQualifiedName>                          type;
    // Specific to property type
    private Optional<Boolean>                                    pii;
    // Specific to entity set property type metadata
    private Optional<Boolean>                                    defaultShow;
    private Optional<String>                                     url;
    private Optional<LinkedHashMap<UUID, LinkedHashSet<String>>> propertyTags;
    private Optional<UUID>                                       organizationId;
    private Optional<LinkedHashSet<Integer>>                     partitions;

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
            @JsonProperty( SerializationConstants.PROPERTY_TAGS )
                    Optional<LinkedHashMap<UUID, LinkedHashSet<String>>> propertyTags,
            @JsonProperty( SerializationConstants.INDEX_TYPE ) Optional<IndexType> indexType,
            @JsonProperty( SerializationConstants.ORGANIZATION_ID ) Optional<UUID> organizationId,
            @JsonProperty( SerializationConstants.PARTITIONS ) Optional<LinkedHashSet<Integer>> partitions,
            @JsonProperty( SerializationConstants.EXPIRATION ) Optional<DataExpiration> dataExpiration ) {
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
        propertyTags.ifPresent( tags ->
                tags.values().forEach( tagValues ->
                        checkArgument( !tagValues.isEmpty(), "Property tag values cannot be empty." )
                )
        );
        this.propertyTags = propertyTags;
        this.indexType = indexType;
        this.organizationId = organizationId;
        this.partitions = partitions;
        this.dataExpiration = dataExpiration;
    }

    public MetadataUpdate(
            Optional<String> title,
            Optional<String> description,
            Optional<String> name,
            Optional<Set<String>> contacts,
            Optional<FullQualifiedName> type,
            Optional<Boolean> pii,
            Optional<Boolean> defaultShow,
            Optional<String> url,
            Optional<LinkedHashMap<UUID, LinkedHashSet<String>>> propertyTags,
            Optional<UUID> organizationId,
            Optional<LinkedHashSet<Integer>> partitions,
            Optional<DataExpiration> dataExpiration ) {
        this( title,
                description,
                name,
                contacts,
                type,
                pii,
                defaultShow,
                url,
                propertyTags,
                Optional.empty(),
                organizationId,
                partitions,
                dataExpiration );
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
    public Optional<LinkedHashMap<UUID, LinkedHashSet<String>>> getPropertyTags() {
        return propertyTags;
    }

    @JsonProperty( SerializationConstants.ORGANIZATION_ID )
    public Optional<UUID> getOrganizationId() {
        return organizationId;
    }

    @JsonProperty( SerializationConstants.INDEX_TYPE )
    public Optional<IndexType> getIndexType() {
        return indexType;
    }

    @JsonProperty( SerializationConstants.PARTITIONS )
    public Optional<LinkedHashSet<Integer>> getPartitions() {
        return partitions;
    }

    @JsonProperty( SerializationConstants.EXPIRATION )
    public Optional<DataExpiration> getDataExpiration() {
        return dataExpiration;
    }

    @Override
    public String toString() {
        return "MetadataUpdate{" +
                "title=" + title +
                ", description=" + description +
                ", indexType=" + indexType +
                ", name=" + name +
                ", contacts=" + contacts +
                ", type=" + type +
                ", pii=" + pii +
                ", defaultShow=" + defaultShow +
                ", url=" + url +
                ", propertyTags=" + propertyTags +
                ", organizationId=" + organizationId +
                ", partitions=" + partitions +
                ", dataExpiration=" + dataExpiration +
                '}';
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof MetadataUpdate ) ) { return false; }
        MetadataUpdate that = ( MetadataUpdate ) o;
        return title.equals( that.title ) &&
                description.equals( that.description ) &&
                indexType.equals( that.indexType ) &&
                name.equals( that.name ) &&
                contacts.equals( that.contacts ) &&
                type.equals( that.type ) &&
                pii.equals( that.pii ) &&
                defaultShow.equals( that.defaultShow ) &&
                url.equals( that.url ) &&
                propertyTags.equals( that.propertyTags ) &&
                organizationId.equals( that.organizationId ) &&
                partitions.equals( that.partitions ) &&
                dataExpiration.equals( that.dataExpiration );
    }

    @Override
    public int hashCode() {
        return Objects.hash( title,
                description,
                indexType,
                name,
                contacts,
                type,
                pii,
                defaultShow,
                url,
                propertyTags,
                organizationId,
                partitions,
                dataExpiration );
    }
}
