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

package com.openlattice.search.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.search.SearchApi;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class Search {

    private final Optional<String>    optionalKeyword;
    private final Optional<UUID>      optionalEntityType;
    private final Optional<Set<UUID>> optionalPropertyTypes;
    private final Optional<UUID>      optionalOrganizationId;
    private final boolean             excludePropertyTypes;
    private final int                 start;
    private final int                 maxHits;

    @JsonCreator
    public Search(
            @JsonProperty( SerializationConstants.KEYWORD ) Optional<String> keyword,
            @JsonProperty( SerializationConstants.ENTITY_TYPE_ID ) Optional<UUID> entityType,
            @JsonProperty( SerializationConstants.PROPERTY_TYPE_IDS ) Optional<Set<UUID>> propertyTypes,
            @JsonProperty( SerializationConstants.ORGANIZATION_ID ) Optional<UUID> organizationId,
            @JsonProperty( SerializationConstants.EXCLUDE_PROPERTY_TYPES ) Optional<Boolean> excludePropertyTypes,
            @JsonProperty( SerializationConstants.START ) int start,
            @JsonProperty( SerializationConstants.MAX_HITS ) int maxHits ) {
        optionalKeyword = keyword.isPresent() ? Optional.of( keyword.get().trim() ) : Optional.empty();
        optionalEntityType = entityType;
        optionalPropertyTypes = propertyTypes;
        optionalOrganizationId = organizationId;
        this.excludePropertyTypes = excludePropertyTypes.orElse( false );
        this.start = start;
        this.maxHits = Math.min( maxHits, SearchApi.MAX_SEARCH_RESULTS );
    }

    @JsonProperty( SerializationConstants.KEYWORD )
    public Optional<String> getOptionalKeyword() {
        return optionalKeyword;
    }

    @JsonProperty( SerializationConstants.ENTITY_TYPE_ID )
    public Optional<UUID> getOptionalEntityType() {
        return optionalEntityType;
    }

    @JsonProperty( SerializationConstants.PROPERTY_TYPE_IDS )
    public Optional<Set<UUID>> getOptionalPropertyTypes() {
        return optionalPropertyTypes;
    }

    @JsonProperty( SerializationConstants.ORGANIZATION_ID )
    public Optional<UUID> getOptionalOrganizationId() {
        return optionalOrganizationId;
    }

    @JsonProperty( SerializationConstants.EXCLUDE_PROPERTY_TYPES )
    public boolean getExcludePropertyTypes() {
        return excludePropertyTypes;
    }

    @JsonProperty( SerializationConstants.START )
    public int getStart() {
        return start;
    }

    @JsonProperty( SerializationConstants.MAX_HITS )
    public int getMaxHits() {
        return maxHits;
    }

}
