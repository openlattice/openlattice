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

import java.util.List;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AdvancedSearch {

    private final List<SearchDetails> searches;
    private final int                 start;
    private final int                 maxHits;

    @JsonCreator
    public AdvancedSearch(
            @JsonProperty( SerializationConstants.SEARCH_FIELDS ) List<SearchDetails> searches,
            @JsonProperty( SerializationConstants.START ) int start,
            @JsonProperty( SerializationConstants.MAX_HITS ) int maxHits ) {
        this.searches = searches;
        this.start = start;
        this.maxHits = maxHits;
    }

    @JsonProperty( SerializationConstants.SEARCH_FIELDS )
    public List<SearchDetails> getSearches() {
        return searches;
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
