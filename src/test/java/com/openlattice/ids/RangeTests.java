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

package com.openlattice.ids;

import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class RangeTests {

    @Test
    public void testExhausted() {
        final Range r = new Range( 1L << 48L, ( 1L << 48 ) - 1, Long.MAX_VALUE );
        Assert.assertNotNull( r.nextId() );
        Assert.assertNull( "Range is exhausted and should return nothing.", r.nextId() );
    }

    @Test
    public void testRollover() {
        final Range r = new Range( 1L << 48L, 1L, Long.MAX_VALUE );
        final UUID id = r.nextId();
        Assert.assertNotNull( id );
        Assert.assertEquals( 1L << 48L | 1L, id.getMostSignificantBits() );
        Assert.assertEquals( Long.MAX_VALUE, id.getLeastSignificantBits() );

        final UUID id2 = r.nextId();
        Assert.assertNotNull( id2 );
        Assert.assertEquals( 1L << 48L | 2L, id2.getMostSignificantBits() );
        Assert.assertEquals( Long.MIN_VALUE, id2.getLeastSignificantBits() );
    }

    @Test
    public void testGetTwoIds() {
        final Range r = new Range( 1L << 48L, 1L, -1L );
        final UUID id = r.nextId();
        Assert.assertNotNull( id );
        Assert.assertEquals( 1L << 48L | 1L, id.getMostSignificantBits() );
        Assert.assertEquals( -1L, id.getLeastSignificantBits() );

        final UUID id2 = r.nextId();
        Assert.assertNotNull( id2 );
        Assert.assertEquals( 1L << 48L | 1L, id2.getMostSignificantBits() );
        Assert.assertEquals( 0, id2.getLeastSignificantBits() );
    }

}
