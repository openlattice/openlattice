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

package com.openlattice.hazelcast.serializers;

import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.openlattice.linking.HazelcastMergingService;
import com.openlattice.linking.aggregators.MergeEdgeAggregator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.Preconditions;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import java.io.IOException;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class MergeEdgeAggregatorStreamSerializer implements SelfRegisteringStreamSerializer<MergeEdgeAggregator> {

    private HazelcastMergingService mergingService;

    @Override public Class<? extends MergeEdgeAggregator> getClazz() {
        return MergeEdgeAggregator.class;
    }

    @Override public void write( ObjectDataOutput out, MergeEdgeAggregator object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getLinkedEntitySetId() );
        UUIDStreamSerializer.serialize( out, object.getSyncId() );
    }

    @Override public MergeEdgeAggregator read( ObjectDataInput in ) throws IOException {
        UUID linkedEntitySetId = UUIDStreamSerializer.deserialize( in );
        UUID syncId = UUIDStreamSerializer.deserialize( in );
        return new MergeEdgeAggregator( linkedEntitySetId, syncId, mergingService );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.MERGE_EDGE_AGGREGATOR.ordinal();
    }

    @Override public void destroy() {

    }

    public synchronized void setMergingService( HazelcastMergingService mergingService ) {
        Preconditions.checkState( this.mergingService == null, "HazelcastMergingService can only be set once" );
        this.mergingService = Preconditions.checkNotNull( mergingService );
    }
}
