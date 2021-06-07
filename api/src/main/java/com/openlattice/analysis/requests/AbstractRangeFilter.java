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

import com.openlattice.analysis.SqlBindInfo;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Objects;

/**
 * A range filter for query property types.
 */
public abstract class AbstractRangeFilter<T extends Comparable<T>> implements RangeFilter<T> {
    protected final T  lowerbound;
    protected final T  upperbound;
    protected final boolean lowerboundEqual;
    protected final boolean upperboundEqual;


    protected AbstractRangeFilter(
            T lowerbound,
            boolean lowerboundEqual,
            T upperbound,
            boolean upperboundEqual ) {
        this.lowerbound = lowerbound;
        this.lowerboundEqual = lowerboundEqual;
        this.upperbound = upperbound;
        this.upperboundEqual = upperboundEqual;
    }

    @Override
    public T getLowerbound() {
        return lowerbound;
    }

    public T getUpperbound() {
        return upperbound;
    }

    public String asSql( String field ) {
        var lowerboundExpr = lowerboundEqual ? field + ">= ?" : field + "> ?";
        var upperboundExpr = upperboundEqual ? field + "<= ?" : field + "< ?";

        return lowerboundExpr + " AND " + upperboundExpr;
    }

    @Override
    @NonNull public LinkedHashSet<SqlBindInfo> bindInfo( int base ) {
        final var lowerboundBindInfo = new SqlBindInfo( base , lowerbound );
        final var upperboundBindInfo = new SqlBindInfo( base + 1, upperbound );

        return new LinkedHashSet<>( Arrays.asList(lowerboundBindInfo, upperboundBindInfo) );
    }

    @Override
    public boolean isLowerboundEqual() {
        return lowerboundEqual;
    }

    @Override
    public boolean isUpperboundEqual() {
        return upperboundEqual;
    }

    protected abstract T getMinValue();

    protected abstract T getMaxValue();

    protected abstract String getLowerboundSql();

    protected abstract String getUpperboundSql();


    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof AbstractRangeFilter ) ) { return false; }
        AbstractRangeFilter<?> that = (AbstractRangeFilter<?>) o;
        return lowerboundEqual == that.lowerboundEqual &&
                upperboundEqual == that.upperboundEqual &&
                Objects.equals( lowerbound, that.lowerbound ) &&
                Objects.equals( upperbound, that.upperbound );
    }

    @Override public String toString() {
        return "AbstractRangeFilter{" +
                "lowerbound=" + lowerbound +
                ", upperbound=" + upperbound +
                ", lowerboundEqual=" + lowerboundEqual +
                ", upperboundEqual=" + upperboundEqual +
                '}';
    }

    @Override public int hashCode() {
        return Objects.hash( lowerbound, upperbound, lowerboundEqual, upperboundEqual );
    }
}
