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
import com.google.common.collect.SetMultimap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.data.aggregators.EntitiesAggregator;
import com.openlattice.data.hazelcast.PropertyKey;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
public class EntitiesAggregatorStreamSerializer implements SelfRegisteringStreamSerializer<EntitiesAggregator> {
    @Override public Class<EntitiesAggregator> getClazz() {
        return EntitiesAggregator.class;
    }

    @Override public void write( ObjectDataOutput out, EntitiesAggregator object ) throws IOException {
        SetMultimap<PropertyKey, ByteBuffer> m = object.getByteBuffers();

        out.writeInt( m.size() );

        for ( Entry<PropertyKey, ByteBuffer> entry : m.entries() ) {
            PropertyKey pk = entry.getKey();

            UUIDStreamSerializer.serialize( out, pk.getId() );
            UUIDStreamSerializer.serialize( out, pk.getPropertyTypeId() );

            out.writeByteArray( entry.getValue().array() );
        }
    }

    @Override public EntitiesAggregator read( ObjectDataInput in ) throws IOException {
        EntitiesAggregator agg = new EntitiesAggregator();
        SetMultimap<PropertyKey, ByteBuffer> m = agg.getByteBuffers();
        int propertyCount = in.readInt();
        for ( int i = 0; i < propertyCount; ++i ) {
            UUID id = UUIDStreamSerializer.deserialize( in );
            UUID propertyTypeId = UUIDStreamSerializer.deserialize( in );
            byte[] bytes = in.readByteArray();
            m.put( new PropertyKey( id, propertyTypeId ), ByteBuffer.wrap( bytes ) );
        }
        return agg;
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ENTITIES_AGGREGATOR.ordinal();
    }

    @Override public void destroy() {

    }
}
