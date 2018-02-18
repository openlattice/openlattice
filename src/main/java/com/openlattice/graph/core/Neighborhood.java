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

package com.openlattice.graph.core;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * In memory representation for efficient aggregation queries.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Neighborhood {
    // vertex id -> dst type id -> edge type id -> dst entity key id -> edge entity key id
    private final Map<UUID, Map<UUID, SetMultimap<UUID, UUID>>> neighborhood;
    //    private final UUID                                          srcEntityKeyId;

    //    public Neighborhood( UUID srcEntityKeyId, Map<UUID, Map<UUID, SetMultimap<UUID, UUID>>> neighborhood ) {
    public Neighborhood( Map<UUID, Map<UUID, SetMultimap<UUID, UUID>>> neighborhood ) {
        this.neighborhood = Preconditions.checkNotNull( neighborhood, "Neighborhood cannot be null" );
        //        this.srcEntityKeyId = srcEntityKeyId;
    }

    public Map<UUID, Map<UUID, SetMultimap<UUID, UUID>>> getNeighborhood() {
        return neighborhood;
    }

    public int count( Set<UUID> dstTypeIds, UUID edgeTypeId, Optional<UUID> dstEntityKeyId ) {
        return ( dstTypeIds.isEmpty() ? neighborhood.values().stream() : dstTypeIds.stream() )
                .map( dstTypeId -> neighborhood.getOrDefault( dstTypeId, ImmutableMap.of() ) )
                .map( dstEntityKeyIds -> dstEntityKeyIds.getOrDefault( edgeTypeId, ImmutableSetMultimap.of() ) )
                .mapToInt( edgeEntityKeyIds ->
                        dstEntityKeyId.<Collection<UUID>>transform( edgeEntityKeyIds::get )
                                .or( edgeEntityKeyIds.values() )
                                .size() )
                .sum();
    }

    //    public UUID getSrcEntityKeyId() {
    //        return srcEntityKeyId;
    //    }

    public Set<UUID> getDstTypeIds() {
        return neighborhood.keySet();
    }

    public Set<UUID> getEdgeTypeIds() {
        return neighborhood.values().stream()
                .map( Map::keySet )
                .flatMap( Set::stream )
                .collect( Collectors.toSet() );
    }

    public Set<UUID> getDstEntityKeyIds() {
        return neighborhood.values().stream()
                .map( Map::values )
                .flatMap( Collection::stream )
                .map( SetMultimap::keySet )
                .flatMap( Set::stream )
                .collect( Collectors.toSet() );
    }

    public Set<UUID> getEdgeEntityKeyIds() {
        return neighborhood.values().stream()
                .map( Map::values )
                .flatMap( Collection::stream )
                .map( Multimaps::asMap )
                .map( Map::values )
                .flatMap( Collection::stream )
                .flatMap( Set::stream )
                .collect( Collectors.toSet() );
    }

    public int count( Set<UUID> dstTypeIds, UUID edgeTypeId ) {
        return count( dstTypeIds, edgeTypeId, Optional.absent() );
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof Neighborhood ) ) { return false; }

        Neighborhood that = (Neighborhood) o;

        return neighborhood.equals( that.neighborhood );
    }

    @Override public int hashCode() {
        return neighborhood.hashCode();
    }

    @VisibleForTesting
    public static Neighborhood randomNeighborhood() {
        SetMultimap<UUID, UUID> sm1 = HashMultimap.create();
        SetMultimap<UUID, UUID> sm2 = HashMultimap.create();
        SetMultimap<UUID, UUID> sm3 = HashMultimap.create();

        for ( int i = 0; i < 5; ++i ) {
            sm1.put( UUID.randomUUID(), UUID.randomUUID() );
            sm2.put( UUID.randomUUID(), UUID.randomUUID() );
            sm3.put( UUID.randomUUID(), UUID.randomUUID() );
        }

        Map<UUID, SetMultimap<UUID, UUID>> m1 = new HashMap<>();
        Map<UUID, SetMultimap<UUID, UUID>> m2 = new HashMap<>();
        Map<UUID, SetMultimap<UUID, UUID>> m3 = new HashMap<>();

        for ( int i = 0; i < 5; ++i ) {
            m1.put( UUID.randomUUID(), sm1 );
            m2.put( UUID.randomUUID(), sm2 );
            m3.put( UUID.randomUUID(), sm3 );
        }

        Map<UUID, Map<UUID, SetMultimap<UUID, UUID>>> neighborhood = new HashMap<>();

        for ( int i = 0; i < 5; ++i ) {
            neighborhood.put( UUID.randomUUID(), m1 );
            neighborhood.put( UUID.randomUUID(), m2 );
            neighborhood.put( UUID.randomUUID(), m3 );
        }

        return new Neighborhood( neighborhood );
    }
}
