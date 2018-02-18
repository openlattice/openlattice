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

import com.openlattice.data.hazelcast.Entities;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
public class EntitiesStreamSerializer implements SelfRegisteringStreamSerializer<Entities> {

    @Override public Class<Entities> getClazz() {
        return Entities.class;
    }

    @Override public void write( ObjectDataOutput out, Entities object ) throws IOException {
        out.writeInt( object.size() );
        for ( Entry<UUID, SetMultimap<UUID, ByteBuffer>> entry : object.entrySet() ) {
            UUIDStreamSerializer.serialize( out, entry.getKey() );
            SetMultimap<UUID, ByteBuffer> mm = entry.getValue();
            out.writeInt( mm.size() );
            for ( Entry<UUID, ByteBuffer> mmEntry : mm.entries() ) {
                UUIDStreamSerializer.serialize( out, mmEntry.getKey() );
                out.writeByteArray( mmEntry.getValue().array() );
            }

        }
    }

    @Override public Entities read( ObjectDataInput in ) throws IOException {
        int entityCount = in.readInt();
        Entities entities = new Entities( entityCount );
        for ( int i = 0; i < entityCount; ++i ) {
            UUID id = UUIDStreamSerializer.deserialize( in );
            int propertyCount = in.readInt();
            SetMultimap<UUID, ByteBuffer> mm = HashMultimap.create();
            for ( int j = 0; j < propertyCount; ++j ) {
                UUID propertyTypeId = UUIDStreamSerializer.deserialize( in );
                mm.put( propertyTypeId, ByteBuffer.wrap( in.readByteArray() ) );
            }
            entities.put( id, mm );
        }
        return entities;
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ENTITIES.ordinal();
    }

    @Override public void destroy() {

    }
}
