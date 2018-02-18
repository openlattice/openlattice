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

import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.graph.edge.EdgeKey;
import java.io.IOException;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
public class EdgeKeyStreamSerializer implements SelfRegisteringStreamSerializer<EdgeKey> {

    @Override public Class<EdgeKey> getClazz() {
        return EdgeKey.class;
    }

    @Override public void write( ObjectDataOutput out, EdgeKey object ) throws IOException {
        serialize( out, object );
    }

    @Override public EdgeKey read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.EDGE_KEY.ordinal();
    }

    @Override public void destroy() {

    }

    public static EdgeKey deserialize( ObjectDataInput in ) throws IOException {
        final UUID srcEntityKeyId = UUIDStreamSerializer.deserialize( in );
        final UUID dstTypeId = UUIDStreamSerializer.deserialize( in );
        final UUID edgeTypeId = UUIDStreamSerializer.deserialize( in );
        final UUID dstEntityKeyId = UUIDStreamSerializer.deserialize( in );
        final UUID edgeEntityKeyId = UUIDStreamSerializer.deserialize( in );

        return new EdgeKey( srcEntityKeyId, dstTypeId, edgeTypeId, dstEntityKeyId, edgeEntityKeyId );
    }

    public static void serialize( ObjectDataOutput out, EdgeKey object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getSrcEntityKeyId() );
        UUIDStreamSerializer.serialize( out, object.getDstTypeId() );
        UUIDStreamSerializer.serialize( out, object.getEdgeTypeId() );
        UUIDStreamSerializer.serialize( out, object.getDstEntityKeyId() );
        UUIDStreamSerializer.serialize( out, object.getEdgeEntityKeyId() );
    }
}
