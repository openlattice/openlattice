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

package com.openlattice.graph.aggregators;

import com.hazelcast.aggregation.Aggregator;
import com.openlattice.graph.core.objects.NeighborTripletSet;
import com.openlattice.graph.edge.Edge;
import com.openlattice.graph.edge.EdgeKey;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDList;
import java.util.HashSet;
import java.util.Map;

public class NeighborEntitySetAggregator extends Aggregator<Map.Entry<EdgeKey, Edge>, NeighborTripletSet> {

    private final NeighborTripletSet edgeTriplets;

    public NeighborEntitySetAggregator() {
        this.edgeTriplets = new NeighborTripletSet( new HashSet<>() );
    }

    public NeighborEntitySetAggregator( NeighborTripletSet edgeTriplets ) {
        this.edgeTriplets = edgeTriplets;
    }

    @Override public void accumulate( Map.Entry<EdgeKey, Edge> input ) {
        DelegatedUUIDList edgeTriplet = new DelegatedUUIDList(
                input.getValue().getSrcSetId(),
                input.getValue().getEdgeSetId(),
                input.getValue().getDstSetId()
        );
        edgeTriplets.add( edgeTriplet );
    }

    @Override public void combine( Aggregator aggregator ) {
        if ( aggregator instanceof NeighborEntitySetAggregator ) {
            edgeTriplets.addAll( ( (NeighborEntitySetAggregator) aggregator ).edgeTriplets );
        }
    }

    @Override public NeighborTripletSet aggregate() {
        return edgeTriplets;
    }

    public NeighborTripletSet getEdgeTriplets() {
        return edgeTriplets;
    }
}
