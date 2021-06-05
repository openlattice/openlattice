/*
 * Copyright (C) 2017. OpenLattice, Inc
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

package com.openlattice.ids;

import java.util.Objects;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: Implement stream serializer if this is ever used for id generation
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Range {
    private static final Logger logger  = LoggerFactory.getLogger( Range.class );
    private static final long   MAX_MSB = 1L << 48;
    private final long base;

    private long msb;
    private long lsb;

    public Range( long base, long msb, long lsb ) {
        this.base = base;
        this.msb = msb;
        this.lsb = lsb;
    }

    public Range( long base ) {
        this( base, 0, Long.MIN_VALUE );
    }

    UUID peek() {
        return  new UUID( base | msb, lsb );
    }

    /**
     * Generates the next id by incrementing the least significant bit
     */
    public UUID nextId() {
        //If we've run out of ids in given range.
        if ( msb == MAX_MSB ) {
            logger.error( "Exhausted id in range with base {} and msb {}", base, msb );
            return null;
        }

        final UUID nextId =  new UUID( base | msb, lsb );

        /*
         * The java specification does not directly guarantee that behavior in case of primitive overflow.
         */
        if ( lsb == Long.MAX_VALUE ) {
            lsb = Long.MIN_VALUE;
            msb++;
        } else {
            lsb++;
        }
        return nextId;
    }

    public long getBase() {
        return base;
    }

    public long getMsb() {
        return msb;
    }

    public long getLsb() {
        return lsb;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof Range ) ) { return false; }
        Range range = (Range) o;
        return base == range.base &&
                msb == range.msb &&
                lsb == range.lsb;
    }

    @Override public int hashCode() {
        return Objects.hash( base, msb, lsb );
    }

    @Override public String toString() {
        return "Range{" +
                "base=" + base +
                ", msb=" + msb +
                ", lsb=" + lsb +
                '}';
    }
}
