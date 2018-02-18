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

package com.openlattice.linking.aggregators;

import com.openlattice.linking.LinkingVertex;
import com.openlattice.linking.LinkingVertexKey;
import com.hazelcast.aggregation.Aggregator;

import com.openlattice.linking.LinkingVertex;
import com.openlattice.linking.LinkingVertexKey;
import java.util.Map;

public class CountVerticesAggregator extends Aggregator<Map.Entry<LinkingVertexKey, LinkingVertex>, Integer>  {
    private int numVertices = 0;

    @Override public void accumulate( Map.Entry<LinkingVertexKey, LinkingVertex> input ) {
        numVertices++;
    }

    @Override public void combine( Aggregator aggregator ) {
        if ( aggregator instanceof CountVerticesAggregator ) {
            numVertices += ( (CountVerticesAggregator) aggregator ).numVertices;
        }
    }

    @Override public Integer aggregate() {
        return numVertices;
    }
}
