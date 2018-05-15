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

import java.util.Set;
import java.util.UUID;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;

public class Search {

    private final Optional<String>    optionalKeyword;
    private final Optional<UUID>      optionalEntityType;
    private final Optional<Set<UUID>> optionalPropertyTypes;
    private final int                 start;
    private final int                 maxHits;

    @JsonCreator
    public Search(
            @JsonProperty( SerializationConstants.KEYWORD ) Optional<String> keyword,
            @JsonProperty( SerializationConstants.ENTITY_TYPE_ID ) Optional<UUID> entityType,
            @JsonProperty( SerializationConstants.PROPERTY_TYPE_IDS ) Optional<Set<UUID>> propertyTypes,
            @JsonProperty( SerializationConstants.START ) int start,
            @JsonProperty( SerializationConstants.MAX_HITS ) int maxHits ) {
        this.optionalKeyword = keyword.isPresent() ? Optional.of( keyword.get().trim() ) : Optional.absent();
        this.optionalEntityType = entityType;
        this.optionalPropertyTypes = propertyTypes;
        this.start = start;
        this.maxHits = maxHits;
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

    @JsonProperty( SerializationConstants.START )
    public int getStart() {
        return start;
    }

    @JsonProperty( SerializationConstants.MAX_HITS )
    public int getMaxHits() {
        return maxHits;
    }

}
