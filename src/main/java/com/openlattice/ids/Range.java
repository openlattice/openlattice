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

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: Implement stream serializer if this is ever used for id generation
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Range {
    private static final Logger logger = LoggerFactory.getLogger( Range.class );

    private long mask;
    private long base;

    private long msb;
    private long lsb;

    public Range( long mask, long base, long msb, long lsb ) {
        this.mask = mask;
        this.base = base;
        this.msb = msb;
        this.lsb = lsb;
    }

    public UUID getNextId() {
        lsb++;

        if ( lsb == 0 ) {
            msb++;
        }

        if ( ( msb & mask ) == 0 ) {
            //We've exhausted ids in this range.
            logger.error( "Uh-oh we ran out ids in a range. That's a lot of objects" );
            return null;
        }

        return new UUID( msb | base, lsb );
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
}
