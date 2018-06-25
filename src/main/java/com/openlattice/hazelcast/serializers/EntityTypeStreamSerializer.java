

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
import com.openlattice.edm.type.EntityType;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

@Component
public class EntityTypeStreamSerializer implements SelfRegisteringStreamSerializer<EntityType> {

    @Override
    public void write( ObjectDataOutput out, EntityType object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public EntityType read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY_TYPE.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<EntityType> getClazz() {
        return EntityType.class;
    }

    public static void serialize( ObjectDataOutput out, EntityType object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getId() );
        FullQualifiedNameStreamSerializer.serialize( out, object.getType() );
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getDescription() );
        SetStreamSerializers.serialize( out, object.getSchemas(), ( FullQualifiedName schema ) -> {
            FullQualifiedNameStreamSerializer.serialize( out, schema );
        } );
        SetStreamSerializers.serialize( out, object.getKey(), ( UUID key ) -> {
            UUIDStreamSerializer.serialize( out, key );
        } );
        SetStreamSerializers.serialize( out, object.getProperties(), ( UUID property ) -> {
            UUIDStreamSerializer.serialize( out, property );
        } );

        final Optional<UUID> baseType = object.getBaseType();
        final boolean present = baseType.isPresent();

        out.writeBoolean( present );

        if ( present ) {
            UUIDStreamSerializer.serialize( out, baseType.get() );
        }
        out.writeUTF( object.getCategory().toString() );
    }

    public static EntityType deserialize( ObjectDataInput in ) throws IOException {
        UUID id = UUIDStreamSerializer.deserialize( in );
        FullQualifiedName type = FullQualifiedNameStreamSerializer.deserialize( in );
        String title = in.readUTF();
        Optional<String> description = Optional.of( in.readUTF() );
        Set<FullQualifiedName> schemas = SetStreamSerializers.deserialize( in, ( ObjectDataInput dataInput ) -> {
            return FullQualifiedNameStreamSerializer.deserialize( dataInput );
        } );
        LinkedHashSet<UUID> keys = SetStreamSerializers.orderedDeserialize( in, UUIDStreamSerializer::deserialize );
        LinkedHashSet<UUID> properties = SetStreamSerializers.orderedDeserialize( in,
                UUIDStreamSerializer::deserialize );
        Optional<UUID> baseType;

        if ( in.readBoolean() ) {
            baseType = Optional.of( UUIDStreamSerializer.deserialize( in ) );
        } else {
            baseType = Optional.empty();
        }
        Optional<SecurableObjectType> category = Optional.of( SecurableObjectType.valueOf( in.readUTF() ) );

        return new EntityType( id, type, title, description, schemas, keys, properties, baseType, category );
    }

}
