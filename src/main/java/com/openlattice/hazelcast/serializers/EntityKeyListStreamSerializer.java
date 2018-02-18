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
import com.openlattice.data.EntityKey;
import java.io.IOException;
import org.springframework.stereotype.Component;

@Component
public class EntityKeyListStreamSerializer implements SelfRegisteringStreamSerializer<EntityKey[]> {
    @Override public Class<? extends EntityKey[]> getClazz() {
        return EntityKey[].class;
    }

    @Override public void write( ObjectDataOutput out, EntityKey[] object ) throws IOException {
        out.writeInt( object.length );
        for (int i = 0; i < object.length; i++ ) {
            EntityKeyStreamSerializer.serialize( out, object[i] );
        }
    }

    @Override public EntityKey[] read( ObjectDataInput in ) throws IOException {
        int size = in.readInt();
        EntityKey[] result = new EntityKey[size];
        for (int i = 0; i < size; i++) {
            result[i] = EntityKeyStreamSerializer.deserialize( in );
        }
        return result;
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY_KEY_ARRAY.ordinal();
    }

    @Override public void destroy() {

    }
}
