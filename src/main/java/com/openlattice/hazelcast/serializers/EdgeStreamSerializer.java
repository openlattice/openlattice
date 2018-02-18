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

package com.openlattice.hazelcast.serializers;

import com.openlattice.graph.edge.Edge;
import com.openlattice.graph.edge.EdgeKey;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import java.io.IOException;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
public class EdgeStreamSerializer implements SelfRegisteringStreamSerializer<Edge> {

    @Override public Class<Edge> getClazz() {
        return Edge.class;
    }

    @Override public void write( ObjectDataOutput out, Edge object ) throws IOException {
        EdgeKeyStreamSerializer.serialize( out, object.getKey() );
        UUIDStreamSerializer.serialize( out, object.getSrcTypeId() );
        UUIDStreamSerializer.serialize( out, object.getSrcSetId() );
        UUIDStreamSerializer.serialize( out, object.getSrcSyncId() );
        UUIDStreamSerializer.serialize( out, object.getDstSetId() );
        UUIDStreamSerializer.serialize( out, object.getDstSyncId() );
        UUIDStreamSerializer.serialize( out, object.getEdgeSetId() );
    }

    @Override public Edge read( ObjectDataInput in ) throws IOException {
        EdgeKey key = EdgeKeyStreamSerializer.deserialize( in );

        UUID srcType = UUIDStreamSerializer.deserialize( in );
        UUID srcSetId = UUIDStreamSerializer.deserialize( in );
        UUID srcSyncId = UUIDStreamSerializer.deserialize( in );
        UUID dstSetId = UUIDStreamSerializer.deserialize( in );
        UUID dstSyncId = UUIDStreamSerializer.deserialize( in );
        UUID edgeSetId = UUIDStreamSerializer.deserialize( in );

        return new Edge( key, srcType, srcSetId, srcSyncId, dstSetId, dstSyncId, edgeSetId );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.LOOM_EDGE.ordinal();
    }

    @Override public void destroy() {

    }
}
