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

import com.openlattice.graph.core.objects.VertexKey;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.data.EntityKey;
import java.io.IOException;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class VertexStreamSerializer implements SelfRegisteringStreamSerializer<VertexKey> {

    @Override
    public Class<? extends VertexKey> getClazz() {
        return VertexKey.class;
    }

    @Override
    public void write( ObjectDataOutput out, VertexKey object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public VertexKey read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.LOOM_VERTEX.ordinal();
    }

    @Override
    public void destroy() {

    }

    public static void serialize( ObjectDataOutput out, VertexKey object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getKey() );
        EntityKeyStreamSerializer.serialize( out, object.getReference() );
    }

    public static VertexKey deserialize( ObjectDataInput in ) throws IOException {
        final UUID key = UUIDStreamSerializer.deserialize( in );
        final EntityKey reference = EntityKeyStreamSerializer.deserialize( in );
        return new VertexKey( key, reference );
    }

}
