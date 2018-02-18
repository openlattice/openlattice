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
import com.openlattice.linking.Entity;
import java.io.IOException;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class EntityStreamSerializer implements SelfRegisteringStreamSerializer<Entity> {
    @Override
    public void write( ObjectDataOutput out, Entity object ) throws IOException {
        EntityKeyStreamSerializer.serialize( out, object.getKey() );
        //TODO Warning: write a map stream serializer?
        out.writeObject( object.getProperties() );
    }

    @Override
    public Entity read( ObjectDataInput in ) throws IOException {
        EntityKey ek = EntityKeyStreamSerializer.deserialize( in );
        //TODO Warning: write a map stream serializer?
        Map<String, Object> properties = in.readObject();
        return new Entity( ek, properties );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY.ordinal();
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<? extends Entity> getClazz() {
        return Entity.class;
    }

}
