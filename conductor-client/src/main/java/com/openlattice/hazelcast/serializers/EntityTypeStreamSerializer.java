

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

import com.google.common.collect.Maps;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.openlattice.mapstores.TestDataFactory;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

@Component
public class EntityTypeStreamSerializer implements TestableSelfRegisteringStreamSerializer<EntityType> {

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
    public void destroy() {
    }

    @Override
    public Class<EntityType> getClazz() {
        return EntityType.class;
    }

    public static void serialize( ObjectDataOutput out, EntityType object ) throws IOException {
        UUIDStreamSerializerUtils.serialize( out, object.getId() );
        FullQualifiedNameStreamSerializer.serialize( out, object.getType() );
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getDescription() );

        SetStreamSerializers.serialize(
                out,
                object.getSchemas(),
                ( FullQualifiedName schema ) -> FullQualifiedNameStreamSerializer.serialize( out, schema )
        );
        SetStreamSerializers.serialize(
                out,
                object.getKey(),
                ( UUID key ) -> UUIDStreamSerializerUtils.serialize( out, key )
        );
        SetStreamSerializers.serialize(
                out,
                object.getProperties(),
                ( UUID property ) -> UUIDStreamSerializerUtils.serialize( out, property )
        );

        SetStreamSerializers.fastUUIDSetSerialize( out, object.getPropertyTags().keySet() );
        for ( LinkedHashSet<String> tags : object.getPropertyTags().values() ) {
            SetStreamSerializers.fastOrderedStringSetSerializeAsArray( out, tags );
        }

        final Optional<UUID> baseType = object.getBaseType();
        final boolean present = baseType.isPresent();

        out.writeBoolean( present );

        if ( present ) {
            UUIDStreamSerializerUtils.serialize( out, baseType.get() );
        }
        out.writeUTF( object.getCategory().toString() );
        out.writeInt( object.getShards() );
    }

    public static EntityType deserialize( ObjectDataInput in ) throws IOException {
        final UUID id = UUIDStreamSerializerUtils.deserialize( in );
        final FullQualifiedName type = FullQualifiedNameStreamSerializer.deserialize( in );
        final String title = in.readUTF();
        final Optional<String> description = Optional.of( in.readUTF() );
        final Set<FullQualifiedName> schemas = SetStreamSerializers.deserialize( in,
                FullQualifiedNameStreamSerializer::deserialize );
        final LinkedHashSet<UUID> keys = SetStreamSerializers.orderedDeserialize(
                in, UUIDStreamSerializerUtils::deserialize
        );
        final LinkedHashSet<UUID> properties = SetStreamSerializers.orderedDeserialize( in,
                UUIDStreamSerializerUtils::deserialize );

        final LinkedHashSet<UUID> propertyTagKeys = SetStreamSerializers.fastOrderedUUIDSetDeserialize( in );
        final LinkedHashMap<UUID, LinkedHashSet<String>> propertyTags =
                Maps.newLinkedHashMapWithExpectedSize( propertyTagKeys.size() );
        for ( UUID propertyTagKey : propertyTagKeys ) {
            propertyTags.put( propertyTagKey, SetStreamSerializers.fastOrderedStringSetDeserializeFromArray( in ) );
        }

        final Optional<UUID> baseType;
        if ( in.readBoolean() ) {
            baseType = Optional.of( UUIDStreamSerializerUtils.deserialize( in ) );
        } else {
            baseType = Optional.empty();
        }
        final Optional<SecurableObjectType> category = Optional.of( SecurableObjectType.valueOf( in.readUTF() ) );
        final Optional<Integer> shards = Optional.of( in.readInt() );

        return new EntityType( id,
                type,
                title,
                description,
                schemas,
                keys,
                properties,
                propertyTags,
                baseType,
                category,
                shards );
    }

    @Override
    public EntityType generateTestValue() {
        return TestDataFactory.entityType();
    }
}
