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

package com.openlattice.linking.aggregators;

import com.openlattice.graph.edge.Edge;
import com.openlattice.graph.edge.EdgeKey;
import com.openlattice.linking.HazelcastMergingService;
import com.hazelcast.aggregation.Aggregator;

import java.util.Map.Entry;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class MergeEdgeAggregator extends Aggregator<Entry<EdgeKey, Edge>, Void> {
    private static final long serialVersionUID = 418792126319526500L;

    private final     UUID                    linkedEntitySetId;
    private final     UUID                    syncId;
    private transient HazelcastMergingService mergingService;

    public MergeEdgeAggregator( UUID linkedEntitySetId, UUID syncId ) {
        this( linkedEntitySetId, syncId, null );
    }

    public MergeEdgeAggregator( UUID linkedEntitySetId, UUID syncId, HazelcastMergingService mergingService ) {
        this.linkedEntitySetId = linkedEntitySetId;
        this.syncId = syncId;
        this.mergingService = mergingService;
    }

    @Override public void accumulate( Entry<EdgeKey, Edge> input ) {
        mergingService.mergeEdgeAsync( linkedEntitySetId, syncId, input.getValue() );
    }

    @Override public void combine( Aggregator aggregator ) {
    }

    @Override public Void aggregate() {
        return null;
    }

    public UUID getLinkedEntitySetId() {
        return linkedEntitySetId;
    }

    public UUID getSyncId() {
        return syncId;
    }

}
