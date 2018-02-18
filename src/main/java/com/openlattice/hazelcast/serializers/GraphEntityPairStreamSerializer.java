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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.blocking.GraphEntityPair;
import java.io.IOException;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class GraphEntityPairStreamSerializer implements SelfRegisteringStreamSerializer<GraphEntityPair> {
    @Override public Class<? extends GraphEntityPair> getClazz() {
        return GraphEntityPair.class;
    }

    @Override public void write( ObjectDataOutput out, GraphEntityPair object ) throws IOException {
        serialize( out, object );
    }

    @Override public GraphEntityPair read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    public static void serialize( ObjectDataOutput out, GraphEntityPair object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getGraphId() );
        UUIDStreamSerializer.serialize( out, object.getEntityKeyId() );
    }

    public static GraphEntityPair deserialize( ObjectDataInput in ) throws IOException {
        UUID graphId = UUIDStreamSerializer.deserialize( in );
        UUID entityKeyId = UUIDStreamSerializer.deserialize( in );
        return new GraphEntityPair( graphId, entityKeyId );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.GRAPH_ENTITY_PAIR.ordinal();
    }

    @Override public void destroy() {

    }
}
