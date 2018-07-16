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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.data.hazelcast.DataKey;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import java.io.IOException;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
public class DataKeyStreamSerializer implements SelfRegisteringStreamSerializer<DataKey> {

    @Override public Class<DataKey> getClazz() {
        return DataKey.class;
    }

    @Override public void write( ObjectDataOutput out, DataKey object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getId() );
        UUIDStreamSerializer.serialize( out, object.getEntitySetId() );
        out.writeUTF( object.getEntityId() );
        UUIDStreamSerializer.serialize( out, object.getPropertyTypeId() );
        out.writeByteArray( object.getHash() );
    }

    @Override public DataKey read( ObjectDataInput in ) throws IOException {
        UUID id = UUIDStreamSerializer.deserialize( in );
        UUID entitySetId = UUIDStreamSerializer.deserialize( in );
        String entityId = in.readUTF();
        UUID propertyTypeId = UUIDStreamSerializer.deserialize( in );
        byte[] hash = in.readByteArray();
        return new DataKey( id, entitySetId, entityId, propertyTypeId, hash );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.DATA_KEY.ordinal();
    }

    @Override public void destroy() {

    }
}
