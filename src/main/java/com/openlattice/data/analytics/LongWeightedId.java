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
 */

package com.openlattice.data.analytics;

import javax.annotation.Nonnull;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class LongWeightedId implements Comparable<LongWeightedId> {
    private final @Nonnull UUID id;
    private final long          weight;

    public LongWeightedId( @Nonnull UUID id, long weight ) {
        this.id = id;
        this.weight = weight;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( !( o instanceof LongWeightedId ) )
            return false;

        LongWeightedId that = (LongWeightedId) o;

        if ( weight != that.weight )
            return false;
        return id.equals( that.id );
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + (int) ( weight ^ ( weight >>> 32 ) );
        return result;
    }

    @Override
    public int compareTo( LongWeightedId o ) {
        int value = Long.compare( weight, o.weight );
        if ( value == 0 ) {
            value = id.compareTo( o.id );
        }
        return value;
    }

    public long getWeight() {
        return weight;
    }

    public UUID getId() {
        return id;
    }
}
