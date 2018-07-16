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
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.edm.type.ComplexType;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

@Component
public class ComplexTypeStreamSerializer implements SelfRegisteringStreamSerializer<ComplexType> {

    @Override
    public void write( ObjectDataOutput out, ComplexType object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getId() );
        FullQualifiedNameStreamSerializer.serialize( out, object.getType() );
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getDescription() );
        SetStreamSerializers.serialize( out,
                object.getSchemas(),
                FullQualifiedNameStreamSerializer::serialize );
        SetStreamSerializers.serialize( out,
                object.getProperties(),
                UUIDStreamSerializer::serialize );
        OptionalStreamSerializers.serialize( out, object.getBaseType(), UUIDStreamSerializer::serialize );
        out.writeUTF( object.getCategory().toString() );
    }

    @Override
    public ComplexType read( ObjectDataInput in ) throws IOException {
        UUID id = UUIDStreamSerializer.deserialize( in );
        FullQualifiedName type = FullQualifiedNameStreamSerializer.deserialize( in );
        String title = in.readUTF();
        Optional<String> description = Optional.of( in.readUTF() );
        Set<FullQualifiedName> schemas = SetStreamSerializers.deserialize( in, FullQualifiedNameStreamSerializer::deserialize );
        LinkedHashSet<UUID> properties = SetStreamSerializers.orderedDeserialize( in,
                UUIDStreamSerializer::deserialize );
        Optional<UUID> baseType = OptionalStreamSerializers.deserialize( in, UUIDStreamSerializer::deserialize );
        SecurableObjectType category = SecurableObjectType.valueOf( in.readUTF() );
        return new ComplexType( id, type, title, description, schemas, properties, baseType, category );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.COMPLEX_TYPE.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<ComplexType> getClazz() {
        return ComplexType.class;
    }

}
