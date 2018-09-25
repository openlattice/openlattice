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

import com.fasterxml.jackson.annotation.JsonIgnore;
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

public class FilteredRankingAggregation {
    private final UUID                                  associationTypeId;
    private final UUID                                  neighborTypeId;
    private final Map<UUID, Set<Filter<?>>>        associationFilters;
    private final Map<UUID, Set<Filter<?>>>        neighborFilters;
    private final Map<UUID, WeightedRankingAggregation> associationAggregations;
    private final Map<UUID, WeightedRankingAggregation> neighborTypeAggregations;
    private final boolean                               isDst;
    private final Optional<Double>                      countWeight;

    @JsonCreator
    public FilteredRankingAggregation(
            @JsonProperty( SerializationConstants.ASSOCIATION_TYPE_ID ) UUID associationTypeId,
            @JsonProperty( SerializationConstants.NEIGHBOR_TYPE_ID ) UUID neighborTypeId,
            @JsonProperty( SerializationConstants.ASSOCIATION_FILTERS )
                    Optional<Map<UUID, Set<Filter<?>>>> associationFilters,
            @JsonProperty( SerializationConstants.NEIGHBOR_FILTERS )
                    Optional<Map<UUID, Set<Filter<?>>>> neighborFilters,
            @JsonProperty( SerializationConstants.ASSOCIATION_AGGREGATIONS )
                    Map<UUID, WeightedRankingAggregation> associationAggregations,
            @JsonProperty( SerializationConstants.ENTITY_SET_AGGREGATIONS )
                    Map<UUID, WeightedRankingAggregation> neighborTypeAggregations,
            @JsonProperty( SerializationConstants.IS_DST ) boolean isDst,
            @JsonProperty( SerializationConstants.WEIGHT ) Optional<Double> countWeight ) {
        this.associationAggregations = associationAggregations;
        this.neighborTypeAggregations = neighborTypeAggregations;
        this.countWeight = countWeight;
        Preconditions.checkNotNull( associationTypeId, "Association type id cannot be null." );
        Preconditions.checkNotNull( neighborTypeId, "Neighbor type ids cannot be null." );
        this.associationTypeId = associationTypeId;
        this.neighborTypeId = neighborTypeId;
        this.associationFilters = associationFilters.orElse( ImmutableMap.of() );
        this.neighborFilters = neighborFilters.orElse( ImmutableMap.of() );
        this.isDst = isDst;
    }

    @JsonProperty( SerializationConstants.ASSOCIATION_TYPE_ID )
    public UUID getAssociationTypeId() {
        return associationTypeId;
    }

    @JsonProperty( SerializationConstants.NEIGHBOR_TYPE_ID )
    public UUID getNeighborTypeId() {
        return neighborTypeId;
    }

    @JsonProperty( SerializationConstants.IS_DST )
    public boolean getDst() {
        return isDst;
    }

    @JsonProperty( SerializationConstants.ASSOCIATION_FILTERS )
    public Map<UUID, Set<Filter<?>>> getAssociationFilters() {
        return associationFilters;
    }

    @JsonProperty( SerializationConstants.NEIGHBOR_FILTERS )
    public Map<UUID, Set<Filter<?>>> getNeighborFilters() {
        return neighborFilters;
    }

    @JsonProperty( SerializationConstants.ASSOCIATION_AGGREGATIONS )
    public Map<UUID, WeightedRankingAggregation> getAssociationAggregations() {
        return associationAggregations;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_AGGREGATIONS )
    public Map<UUID, WeightedRankingAggregation> getNeighborTypeAggregations() {
        return neighborTypeAggregations;
    }

    public Optional<Double> getCountWeight() {
        return countWeight;
    }

    @JsonProperty( SerializationConstants.WEIGHT )

    @JsonIgnore
    public boolean isCount() {
        return countWeight.isPresent();
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof FilteredRankingAggregation ) ) { return false; }
        FilteredRankingAggregation that = (FilteredRankingAggregation) o;
        return isDst == that.isDst &&
                Objects.equals( associationTypeId, that.associationTypeId ) &&
                Objects.equals( neighborTypeId, that.neighborTypeId ) &&
                Objects.equals( associationFilters, that.associationFilters ) &&
                Objects.equals( neighborFilters, that.neighborFilters ) &&
                Objects.equals( associationAggregations, that.associationAggregations ) &&
                Objects.equals( neighborTypeAggregations, that.neighborTypeAggregations );
    }

    @Override public int hashCode() {
        return Objects.hash( associationTypeId,
                neighborTypeId,
                associationFilters,
                neighborFilters,
                associationAggregations,
                neighborTypeAggregations,
                isDst );
    }

    @Override public String toString() {
        return "FilteredRankingAggregation{" +
                "associationTypeId=" + associationTypeId +
                ", neighborTypeId=" + neighborTypeId +
                ", associationFilters=" + associationFilters +
                ", neighborFilters=" + neighborFilters +
                ", associationAggregations=" + associationAggregations +
                ", neighborTypeAggregations=" + neighborTypeAggregations +
                ", isDst=" + isDst +
                '}';
    }
}
