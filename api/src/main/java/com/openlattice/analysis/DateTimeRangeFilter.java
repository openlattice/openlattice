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
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.Optional;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DateTimeRangeFilter extends AbstractRangeFilter<OffsetDateTime> {

    @JsonCreator
    public DateTimeRangeFilter(
            @JsonProperty( SerializationConstants.LOWERBOUND ) Optional<OffsetDateTime> lowerbound,
            @JsonProperty( SerializationConstants.GTE ) Optional<Boolean> lowerboundEqual,
            @JsonProperty( SerializationConstants.UPPERBOUND ) Optional<OffsetDateTime> upperbound,
            @JsonProperty( SerializationConstants.LTE ) Optional<Boolean> upperboundEqual ) {
        this( lowerbound.orElse( OffsetDateTime.MIN ),
                lowerboundEqual.orElse( false ),
                upperbound.orElse( OffsetDateTime.MAX ),
                upperboundEqual.orElse( false ) );
    }

    protected DateTimeRangeFilter(
            OffsetDateTime lowerbound,
            boolean lowerboundEqual,
            OffsetDateTime upperbound,
            boolean upperboundEqual ) {
        super( lowerbound, lowerboundEqual, upperbound, upperboundEqual );
    }

    @Override protected OffsetDateTime getMinValue() {
        return OffsetDateTime.MIN;
    }

    @Override protected OffsetDateTime getMaxValue() {
        return OffsetDateTime.MAX;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof AbstractRangeFilter ) ) { return false; }
        AbstractRangeFilter<?> that = (AbstractRangeFilter<?>) o;
        return lowerboundEqual == that.lowerboundEqual &&
                upperboundEqual == that.upperboundEqual &&
                lowerbound.isEqual( (OffsetDateTime) that.lowerbound ) &&
                upperbound.isEqual( (OffsetDateTime) that.upperbound );
    }

    public static DateTimeRangeFilter greaterThan( OffsetDateTime value ) {
        return new DateTimeRangeFilter( value, false, OffsetDateTime.MAX, false );
    }

    public static DateTimeRangeFilter greaterThanOrEqual( OffsetDateTime value ) {
        return new DateTimeRangeFilter( value, true, OffsetDateTime.MAX, true );
    }

    public static DateTimeRangeFilter lessThan( OffsetDateTime value ) {
        return new DateTimeRangeFilter( OffsetDateTime.MIN, true, value, false );
    }

    public static DateTimeRangeFilter lessThanEqual( OffsetDateTime value ) {
        return new DateTimeRangeFilter( OffsetDateTime.MIN, true, value, true );
    }

    public static DateTimeRangeFilter betweenInc( OffsetDateTime lowerbound, OffsetDateTime upperbound ) {
        return new DateTimeRangeFilter( lowerbound, true, upperbound, true );
    }

    public static DateTimeRangeFilter betweenExcl( OffsetDateTime lowerbound, OffsetDateTime upperbound ) {
        return new DateTimeRangeFilter( lowerbound, false, upperbound, false );
    }
}
