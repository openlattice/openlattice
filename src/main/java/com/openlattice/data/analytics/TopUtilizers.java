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

import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class TopUtilizers {
    private final int                                   limit;
    private final ConcurrentSkipListSet<LongWeightedId> utilizers;

    public TopUtilizers( int limit ) {
        Preconditions.checkArgument( limit > 0, "Number of top utilizers must be non-zero" );
        this.limit = limit;
        this.utilizers = new ConcurrentSkipListSet<LongWeightedId>();
    }

    public void accumulate( LongWeightedId id ) {
        if ( utilizers.size() < limit ) {
            utilizers.add( id );
        } else {
            if ( utilizers.first().getWeight() < id.getWeight() ) {
                utilizers.add( id );
                utilizers.pollFirst();
            }
        }
    }

    public Stream<LongWeightedId> stream() {
        return utilizers.descendingSet().stream();
    }

    public void accumulate( UUID vertexId, long score ) {
        accumulate( new LongWeightedId( vertexId, score ) );
    }
}
