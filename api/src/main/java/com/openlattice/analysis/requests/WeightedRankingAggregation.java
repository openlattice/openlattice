/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.analysis.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;
import java.util.Objects;
import java.util.UUID;

/**
 * Model for specifying weight ranking aggregation.
 */
public class WeightedRankingAggregation {
    private final AggregationType type;
    private final double          weight;

    @JsonCreator
    public WeightedRankingAggregation(
            @JsonProperty( SerializationConstants.AGGREGATION_TYPE ) AggregationType type,
            @JsonProperty( SerializationConstants.WEIGHT ) double weight ) {
        this.type = type;
        this.weight = weight;
    }

    @JsonProperty( SerializationConstants.AGGREGATION_TYPE )
    public AggregationType getType() {
        return type;
    }

    @JsonProperty( SerializationConstants.WEIGHT )
    public double getWeight() {
        return weight;
    }

    @Override public boolean equals( Object o ) {

        if ( this == o ) { return true; }
        if ( !( o instanceof WeightedRankingAggregation ) ) { return false; }
        WeightedRankingAggregation that = (WeightedRankingAggregation) o;
        return Double.compare( that.weight, weight ) == 0 &&
                type == that.type;
    }

    @Override public int hashCode() {
        return Objects.hash( type, weight );
    }

    @Override public String toString() {
        return "WeightedRankingAggregation{" +
                "type=" + type +
                ", weight=" + weight +
                '}';
    }
}
