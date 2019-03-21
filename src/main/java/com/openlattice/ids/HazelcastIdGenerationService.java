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

import com.google.common.collect.ImmutableMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.hazelcast.HazelcastMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
public class HazelcastIdGenerationService {
    /*
     * This should be good enough until we scale past 65536 Hazelcast nodes.
     */
    public static final  int               MASK_LENGTH    = 16;
    public static final  int               NUM_PARTITIONS = 1 << MASK_LENGTH; //65536
    private static final Random            r              = new Random();
    /*
     * Each range owns a portion of the keyspace.
     */
    private final        IMap<Long, Range> scrolls;
    private final        AtomicInteger     rangeIndex     = new AtomicInteger();

    public HazelcastIdGenerationService( HazelcastInstance hazelcastInstance ) {
        this.scrolls = hazelcastInstance.getMap( HazelcastMap.ID_GENERATION.name() );
        if ( scrolls.isEmpty() ) {
            initializeRanges();
        }
    }

    public void initializeRanges() {
        final Map<Long, Range> ranges = new HashMap<>(NUM_PARTITIONS);
        for ( long i = 0; i < NUM_PARTITIONS; ++i ) {
            ranges.put( i, new Range( i << 48L ) );
        }
        scrolls.putAll( ranges );
    }

    public Set<UUID> getNextIds( int count ) {
        final int remainderToBeDistributed = count % NUM_PARTITIONS;
        final int countPerPartition = count / NUM_PARTITIONS; //0 if count < NUM_PARTITIONS
        final Set<Long> ranges = new HashSet<>( remainderToBeDistributed );

        while ( ranges.size() < remainderToBeDistributed ) {
            ranges.add( ( (long) r.nextInt( NUM_PARTITIONS ) ) );
        }

        final Map<Long, Object> ids;

        if ( countPerPartition > 0 ) {
            ids = scrolls
                    .executeOnEntries( new IdGeneratingEntryProcessor( countPerPartition ) );
        } else {
            ids = ImmutableMap.of();
        }

        final Map<Long, Object> remainingIds = scrolls.executeOnKeys( ranges, new IdGeneratingEntryProcessor( 1 ) );

        return Stream
                .concat( ids.values().stream(), remainingIds.values().stream() )
                .map( idList -> (List<UUID>) idList )
                .flatMap( List::stream )
                .collect( Collectors.toSet() );

    }

    public UUID nextId() {
        return getNextIds( 1 ).iterator().next();
    }
}
