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

package com.openlattice.analysis.requests;

import com.google.common.collect.ImmutableMap;
import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class FilteredRanking {
    private final UUID                        associationTypeId;
    private final UUID                        neighborTypeId;
    private final Map<UUID, Set<RangeFilter<?>>> associationFilters;
    private final Map<UUID, Set<RangeFilter<?>>> neighborFilters;
    private final boolean                     utilizerIsSrc;

    @JsonCreator
    public FilteredRanking(
            @JsonProperty( SerializationConstants.ASSOCIATION_TYPE_ID ) UUID associationTypeId,
            @JsonProperty( SerializationConstants.NEIGHBOR_TYPE_ID ) UUID neighborTypeId,
            @JsonProperty( SerializationConstants.ASSOCIATION_FILTERS )
                    Optional<Map<UUID, Set<RangeFilter<?>>>> associationFilters,
            @JsonProperty( SerializationConstants.NEIGHBOR_FILTERS )
                    Optional<Map<UUID, Set<RangeFilter<?>>>> neighborFilters,
            @JsonProperty( SerializationConstants.UTILIZER_IS_SRC ) boolean utilizerIsSrc ) {
        Preconditions.checkNotNull( associationTypeId, "Association type id cannot be null." );
        Preconditions.checkNotNull( neighborTypeId, "Neighbor type ids cannot be null." );
        this.associationTypeId = associationTypeId;
        this.neighborTypeId = neighborTypeId;
        this.associationFilters = associationFilters.orElse( ImmutableMap.of() );
        this.neighborFilters = neighborFilters.orElse( ImmutableMap.of() );
        this.utilizerIsSrc = utilizerIsSrc;
    }

    @JsonProperty( SerializationConstants.ASSOCIATION_TYPE_ID )
    public UUID getAssociationTypeId() {
        return associationTypeId;
    }

    @JsonProperty( SerializationConstants.NEIGHBOR_TYPE_ID )
    public UUID getNeighborTypeId() {
        return neighborTypeId;
    }

    @JsonProperty( SerializationConstants.UTILIZER_IS_SRC )
    public boolean getUtilizerIsSrc() {
        return utilizerIsSrc;
    }

    @JsonProperty( SerializationConstants.ASSOCIATION_FILTERS )
    public Map<UUID, Set<RangeFilter<?>>> getAssociationFilters() {
        return associationFilters;
    }

    @JsonProperty( SerializationConstants.NEIGHBOR_FILTERS )
    public Map<UUID, Set<RangeFilter<?>>> getNeighborFilters() {
        return neighborFilters;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof FilteredRanking ) ) { return false; }
        FilteredRanking that = (FilteredRanking) o;
        return utilizerIsSrc == that.utilizerIsSrc &&
                Objects.equals( associationTypeId, that.associationTypeId ) &&
                Objects.equals( neighborTypeId, that.neighborTypeId ) &&
                Objects.equals( associationFilters, that.associationFilters ) &&
                Objects.equals( neighborFilters, that.neighborFilters );
    }

    @Override public int hashCode() {
        return Objects.hash( associationTypeId, neighborTypeId, associationFilters, neighborFilters, utilizerIsSrc );
    }
}
