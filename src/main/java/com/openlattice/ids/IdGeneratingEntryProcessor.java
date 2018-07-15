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

import com.google.common.annotations.VisibleForTesting;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class IdGeneratingEntryProcessor extends AbstractRhizomeEntryProcessor<Integer, Range, List<UUID>> {
    private final int count;

    public IdGeneratingEntryProcessor( int count ) {
        this.count = count;
    }

    @Override
    public List<UUID> process( Entry<Integer, Range> entry ) {
        final Range range = entry.getValue(); //Range should never be null in the EP.
        final UUID[] ids = getIds( range );
        entry.setValue( range );
        return Arrays.asList( ids );
    }

    public int getCount() {
        return count;
    }

    @VisibleForTesting
    public UUID[] getIds( Range range ) {
        final UUID[] ids = new UUID[ count ];

        for ( int i = 0; i < ids.length; ++i ) {
            ids[ i ] = range.nextId();
        }
        return ids;
    }
}

