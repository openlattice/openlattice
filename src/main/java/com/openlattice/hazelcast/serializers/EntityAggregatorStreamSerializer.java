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

import com.openlattice.data.aggregators.EntityAggregator;
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
public class EntityAggregatorStreamSerializer implements SelfRegisteringStreamSerializer<EntityAggregator> {
    @Override public Class<EntityAggregator> getClazz() {
        return EntityAggregator.class;
    }

    @Override public void write( ObjectDataOutput out, EntityAggregator object ) throws IOException {
        SetMultimap<UUID, ByteBuffer> mm = object.getByteBuffers();
        out.writeInt( mm.size() );
        for ( Entry<UUID, ByteBuffer> entry : mm.entries() ) {
            UUIDStreamSerializer.serialize( out, entry.getKey() );
            out.writeByteArray( entry.getValue().array() );
        }
    }

    @Override public EntityAggregator read( ObjectDataInput in ) throws IOException {
        SetMultimap<UUID, ByteBuffer> mm = HashMultimap.create();
        int size = in.readInt();
        for ( int i = 0; i < size; ++i ) {

            UUID id = UUIDStreamSerializer.deserialize( in );
            ByteBuffer bb = ByteBuffer.wrap( in.readByteArray() );
            mm.put( id, bb );
        }
        return new EntityAggregator( mm );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY_AGGREGATOR.ordinal();
    }

    @Override public void destroy() {

    }
}
