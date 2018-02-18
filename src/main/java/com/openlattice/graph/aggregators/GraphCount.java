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

package com.openlattice.graph.aggregators;

import com.hazelcast.aggregation.Aggregator;
import com.openlattice.data.analytics.IncrementableWeightId;
import com.openlattice.graph.edge.Edge;
import com.openlattice.graph.edge.EdgeKey;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

/**
 * This aggregator assume that EdgeKey is partition aware so that all necessary edges will be counted
 * within a particular partition.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class GraphCount extends Aggregator<Entry<EdgeKey, Edge>, IncrementableWeightId[]> {
    private final int                              limit;
    private final Map<UUID, IncrementableWeightId> weightedIds;
    private final UUID                             entitySetId; //true => agg(src) -> dst , false => src <- agg(dst)

    public GraphCount( int limit, UUID entitySetId ) {
        this( limit, entitySetId, new HashMap<>() );
    }

    public GraphCount( int limit, UUID entitySetId, Map<UUID, IncrementableWeightId> weightedIds ) {
        this.weightedIds = weightedIds;
        this.limit = limit;
        this.entitySetId = entitySetId;
    }

    @Override
    public void accumulate( Entry<EdgeKey, Edge> input ) {
        final UUID key = input.getValue().getSrcSetId().equals( entitySetId ) ?
                input.getKey().getSrcEntityKeyId() : input.getKey().getDstEntityKeyId();

        IncrementableWeightId newWeightId = new IncrementableWeightId( key, 0 );
        IncrementableWeightId existingWeightId = weightedIds.putIfAbsent( key, newWeightId );

        ( existingWeightId == null ? newWeightId : existingWeightId ).increment();
    }

    @Override
    public void combine( Aggregator aggregator ) {
        GraphCount gc = (GraphCount) aggregator;
        gc.weightedIds.forEach( ( id, weight ) -> weightedIds.merge( id, weight, IncrementableWeightId::merge ) );

        //Shrink if above limit after merge
        //        if ( weightedIds.size() > limit ) {
        //            weightedIds.values().stream()
        //                    .sorted( Comparator.reverseOrder() )
        //                    .skip( limit )
        //                    .map( IncrementableWeightId::getId )
        //                    .forEach( weightedIds::remove );
        //        }
    }

    @Override
    public IncrementableWeightId[] aggregate() {
        return weightedIds.values().stream()
                .sorted( Comparator.reverseOrder() )
                .limit( limit )
                .toArray( IncrementableWeightId[]::new );
    }

    public UUID getEntitySetId() {
        return entitySetId;
    }

    public Map<UUID, IncrementableWeightId> getWeightedIds() {
        return weightedIds;
    }

    public int getLimit() {
        return limit;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof GraphCount ) ) { return false; }

        GraphCount that = (GraphCount) o;

        if ( limit != that.limit ) { return false; }
        return weightedIds.equals( that.weightedIds );
    }

    @Override public int hashCode() {
        int result = limit;
        result = 31 * result + weightedIds.hashCode();
        return result;
    }

    @Override public String toString() {
        return "GraphCount{" +
                "limit=" + limit +
                ", weightedIds=" + weightedIds +
                '}';
    }
}
