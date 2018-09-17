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

import java.util.List;
import java.util.Objects;

/**
 * Model for requesting a weighted ranking aggregation.
 */
public class WeightedRankingAggregation {
    private final List<FilteredRanking> rankings;
    private final List<Double>          weights;

    public WeightedRankingAggregation( List<FilteredRanking> rankings, List<Double> weights ) {
        this.rankings = rankings;
        this.weights = weights;
    }

    public List<FilteredRanking> getRankings() {
        return rankings;
    }

    public List<Double> getWeights() {
        return weights;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof WeightedRankingAggregation ) ) { return false; }
        WeightedRankingAggregation that = (WeightedRankingAggregation) o;
        return Objects.equals( rankings, that.rankings ) &&
                Objects.equals( weights, that.weights );
    }

    @Override public int hashCode() {
        return Objects.hash( rankings, weights );
    }

    @Override public String toString() {
        return "WeightedRankingAggregation{" +
                "rankings=" + rankings +
                ", weights=" + weights +
                '}';
    }
}
