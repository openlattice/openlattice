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
import com.google.common.collect.Sets;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.graph.core.objects.NeighborTripletSet;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDList;
import java.io.IOException;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class NeighborTripletSetStreamSerializer implements SelfRegisteringStreamSerializer<NeighborTripletSet> {
    @Override public Class<? extends NeighborTripletSet> getClazz() {
        return NeighborTripletSet.class;
    }

    @Override public void write( ObjectDataOutput out, NeighborTripletSet object ) throws IOException {
        serialize( out, object );
    }

    @Override public NeighborTripletSet read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.NEIGHBOR_TRIPLET_SET.ordinal();
    }

    @Override public void destroy() {

    }

    public static void serialize( ObjectDataOutput out, NeighborTripletSet object ) throws IOException {
        out.writeInt( object.size() );
        for ( DelegatedUUIDList triplet : object ) {
            UUIDStreamSerializer.serialize( out, triplet.get( 0 ) );
            UUIDStreamSerializer.serialize( out, triplet.get( 1 ) );
            UUIDStreamSerializer.serialize( out, triplet.get( 2 ) );
        }
    }

    public static NeighborTripletSet deserialize( ObjectDataInput in ) throws IOException {
        NeighborTripletSet result = new NeighborTripletSet( Sets.newHashSet() );
        int size = in.readInt();
        for ( int i = 0; i < size; i++ ) {
            UUID src = UUIDStreamSerializer.deserialize( in );
            UUID assoc = UUIDStreamSerializer.deserialize( in );
            UUID dst = UUIDStreamSerializer.deserialize( in );
            result.add( new DelegatedUUIDList( src, assoc, dst ) );
        }
        return result;
    }
}
