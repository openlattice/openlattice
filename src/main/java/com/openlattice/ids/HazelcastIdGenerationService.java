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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class HazelcastIdGenerationService {
    /*
     * This should be good enough until we scale past 65536 Hazelcast nodes.
     */
    public static final int  MASK_LENGTH    = 16;
    public static final int  NUM_PARTITIONS = 1 << MASK_LENGTH; //65536
    public static final long MASK           = ( -1L ) >>> MASK_LENGTH; //
    /*
     * Each range owns a portion of the unassigned keyspace. It can be split
     */
    private final IMap<Integer, Range> scrolls;
    private final AtomicInteger rangeIndex = new AtomicInteger();

    public HazelcastIdGenerationService( HazelcastInstance hazelcastInstance ) {
        this.scrolls = hazelcastInstance.getMap( "" );
        if ( scrolls.isEmpty() ) {
            initializeRanges();
        }
    }

    public void initializeRanges() {
        for ( int i = 0; i < NUM_PARTITIONS; ++i ) {
            new Range( MASK, i * ( 1L << ( 64 - MASK_LENGTH ) ), 0, 0 );
        }
    }

    public Set<UUID> getNextIds( int count ) {
        int start = rangeIndex.getAndAdd( count );
        Map<Integer, Object> idMap = scrolls
                .executeOnEntries( new IdGeneratingEntryProcessor(), between( start, start + count - 1 ) );
        Set<UUID> ids = new HashSet<>( count );
        idMap.forEach( ( k, v ) -> ids.add( (UUID) v ) );
        return ids;
    }

    public UUID nextId() {
        return (UUID) scrolls.executeOnKey( rangeIndex.getAndIncrement(), new IdGeneratingEntryProcessor() );

    }

    public static Predicate between( int start, int end ) {
        return Predicates.between( "__key", start, end );
    }
}
