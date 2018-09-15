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
import java.time.LocalDate;
import java.time.chrono.ChronoLocalDate;
import java.util.Optional;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DateRangeFilter extends AbstractRangeFilter<ChronoLocalDate> {

    @JsonCreator
    public DateRangeFilter(
            @JsonProperty( SerializationConstants.LOWERBOUND ) Optional<LocalDate> lowerbound,
            @JsonProperty( SerializationConstants.GTE ) Optional<Boolean> lowerboundEqual,
            @JsonProperty( SerializationConstants.UPPERBOUND ) Optional<LocalDate> upperbound,
            @JsonProperty( SerializationConstants.LTE ) Optional<Boolean> upperboundEqual ) {
        this( lowerbound.orElse( LocalDate.MIN ),
                lowerboundEqual.orElse( false ),
                upperbound.orElse( LocalDate.MAX ),
                upperboundEqual.orElse( false ) );
    }

    protected DateRangeFilter(
            LocalDate lowerbound,
            boolean lowerboundEqual,
            LocalDate upperbound,
            boolean upperboundEqual ) {
        super( lowerbound, lowerboundEqual, upperbound, upperboundEqual );
    }

    @Override protected LocalDate getMinValue() {
        return LocalDate.MIN;
    }

    @Override protected LocalDate getMaxValue() {
        return LocalDate.MAX;
    }

    public static DateRangeFilter greaterThan( LocalDate value ) {
        return new DateRangeFilter( value, false, LocalDate.MIN, false );
    }

    public static DateRangeFilter greaterThanOrEqual( LocalDate value ) {
        return new DateRangeFilter( value, true, LocalDate.MAX, true );
    }

    public static DateRangeFilter lessThan( LocalDate value ) {
        return new DateRangeFilter( LocalDate.MIN, true, value, false );
    }

    public static DateRangeFilter lessThanEqual( LocalDate value ) {
        return new DateRangeFilter( LocalDate.MIN, true, value, true );
    }

    public static DateRangeFilter betweenInc( LocalDate lowerbound, LocalDate upperbound ) {
        return new DateRangeFilter( lowerbound, true, upperbound, true );
    }

    public static DateRangeFilter betweenExcl( LocalDate lowerbound, LocalDate upperbound ) {
        return new DateRangeFilter( lowerbound, false, upperbound, false );
    }
}
