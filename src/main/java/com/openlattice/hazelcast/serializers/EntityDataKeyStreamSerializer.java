

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

import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.data.EntityDataKey;
import com.openlattice.edm.type.EntityType;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

@Component
public class EntityDataKeyStreamSerializer implements SelfRegisteringStreamSerializer<EntityDataKey> {

    @Override
    public void write( ObjectDataOutput out, EntityDataKey object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public EntityDataKey read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY_DATA_KEY.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<EntityDataKey> getClazz() {
        return EntityDataKey.class;
    }

    public static void serialize( ObjectDataOutput out, EntityDataKey object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getEntityKeyId() );
        UUIDStreamSerializer.serialize( out, object.getEntitySetId() );
    }

    public static EntityDataKey deserialize( ObjectDataInput in ) throws IOException {
        UUID entityKeyId = UUIDStreamSerializer.deserialize( in );
        UUID entitySetId = UUIDStreamSerializer.deserialize( in );
        return new EntityDataKey( entitySetId, entityKeyId );
    }

}
