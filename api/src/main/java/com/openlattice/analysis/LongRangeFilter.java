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

package com.openlattice.analysis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;
import java.util.Optional;

/**
 * Represent a filter
 */
public class LongRangeFilter extends AbstractRangeFilter<Long> {

    @JsonCreator
    public LongRangeFilter(
            @JsonProperty( SerializationConstants.LOWERBOUND ) Optional<Long> lowerbound,
            @JsonProperty( SerializationConstants.GTE ) Optional<Boolean> lowerboundEqual,
            @JsonProperty( SerializationConstants.UPPERBOUND ) Optional<Long> upperbound,
            @JsonProperty( SerializationConstants.LTE ) Optional<Boolean> upperboundEqual ) {
        this( lowerbound.orElse( Long.MIN_VALUE ),
                lowerboundEqual.orElse( false ),
                upperbound.orElse( Long.MAX_VALUE ),
                upperboundEqual.orElse( false ) );
    }

    protected LongRangeFilter(
            Long lowerbound,
            boolean lowerboundEqual,
            Long upperbound,
            boolean upperboundEqual ) {
        super( lowerbound, lowerboundEqual, upperbound, upperboundEqual );
    }

    @Override protected Long getMinValue() {
        return Long.MIN_VALUE;
    }

    @Override protected Long getMaxValue() {
        return Long.MAX_VALUE;
    }

    public static LongRangeFilter greaterThan( Long value ) {
        return new LongRangeFilter( value, false, Long.MAX_VALUE, false );
    }

    public static LongRangeFilter greaterThanOrEqual( Long value ) {
        return new LongRangeFilter( value, true, Long.MAX_VALUE, true );
    }

    public static LongRangeFilter lessThan( Long value ) {
        return new LongRangeFilter( Long.MIN_VALUE, true, value, false );
    }

    public static LongRangeFilter lessThanEqual( Long value ) {
        return new LongRangeFilter( Long.MIN_VALUE, true, value, true );
    }

    public static LongRangeFilter betweenInc( Long lowerbound, Long upperbound ) {
        return new LongRangeFilter( lowerbound, true, upperbound, true );
    }

    public static LongRangeFilter betweenExcl( Long lowerbound, Long upperbound ) {
        return new LongRangeFilter( lowerbound, false, upperbound, false );
    }
}
