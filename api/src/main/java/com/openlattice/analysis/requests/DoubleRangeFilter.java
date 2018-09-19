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
import java.util.Optional;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DoubleRangeFilter extends AbstractRangeFilter<Double> {

    @JsonCreator
    public DoubleRangeFilter(
            @JsonProperty( SerializationConstants.LOWERBOUND ) Optional<Double> lowerbound,
            @JsonProperty( SerializationConstants.GTE ) Optional<Boolean> lowerboundEqual,
            @JsonProperty( SerializationConstants.UPPERBOUND ) Optional<Double> upperbound,
            @JsonProperty( SerializationConstants.LTE ) Optional<Boolean> upperboundEqual ) {
        this( lowerbound.orElse( Double.MIN_VALUE ),
                lowerboundEqual.orElse( false ),
                upperbound.orElse( Double.MAX_VALUE ),
                upperboundEqual.orElse( false ) );
    }

    protected DoubleRangeFilter(
            Double lowerbound,
            boolean lowerboundEqual,
            Double upperbound,
            boolean upperboundEqual ) {
        super( lowerbound, lowerboundEqual, upperbound, upperboundEqual );
    }

    @Override protected Double getMinValue() {
        return Double.MIN_VALUE;
    }

    @Override protected Double getMaxValue() {
        return Double.MAX_VALUE;
    }

    @Override protected String getLowerboundSql() {
        return lowerbound.toString();
    }

    @Override protected String getUpperboundSql() {
        return upperbound.toString();
    }

    public static DoubleRangeFilter greaterThan( Double value ) {
        return new DoubleRangeFilter( value, false, Double.MAX_VALUE, false );
    }

    public static DoubleRangeFilter greaterThanOrEqual( Double value ) {
        return new DoubleRangeFilter( value, true, Double.MAX_VALUE, true );
    }

    public static DoubleRangeFilter lessThan( Double value ) {
        return new DoubleRangeFilter( Double.MIN_VALUE, true, value, false );
    }

    public static DoubleRangeFilter lessThanEqual( Double value ) {
        return new DoubleRangeFilter( Double.MIN_VALUE, true, value, true );
    }

    public static DoubleRangeFilter betweenInc( Double lowerbound, Double upperbound ) {
        return new DoubleRangeFilter( lowerbound, true, upperbound, true );
    }

    public static DoubleRangeFilter betweenExcl( Double lowerbound, Double upperbound ) {
        return new DoubleRangeFilter( lowerbound, false, upperbound, false );
    }
}
