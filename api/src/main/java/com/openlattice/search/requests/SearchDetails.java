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

import java.io.Serializable;
import java.util.UUID;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class SearchDetails implements Serializable {
    private static final long serialVersionUID = -6691065251875288358L;

    private final String searchTerm;
    private final UUID propertyType;
    private final boolean exactMatch;
    
    @JsonCreator
    public SearchDetails(
            @JsonProperty( SerializationConstants.SEARCH_TERM ) String searchTerm,
            @JsonProperty( SerializationConstants.PROPERTY_FIELD ) UUID propertyType,
            @JsonProperty( SerializationConstants.EXACT ) boolean exactMatch ) {
        Preconditions.checkNotNull( searchTerm );
        Preconditions.checkNotNull( propertyType );

        this.searchTerm = searchTerm;
        this.propertyType = propertyType;
        this.exactMatch = exactMatch;
    }
    
    @JsonProperty( SerializationConstants.SEARCH_TERM )
    public String getSearchTerm() {
        return searchTerm;
    }
    
    @JsonProperty( SerializationConstants.PROPERTY_FIELD )
    public UUID getPropertyType() {
        return propertyType;
    }
    
    @JsonProperty( SerializationConstants.EXACT )
    public boolean getExactMatch() {
        return exactMatch;
    }

}
