

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

import com.esotericsoftware.kryo.Kryo;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.data.EntityDataMetadata;
import com.openlattice.data.EntityDataValue;
import com.openlattice.data.PropertyMetadata;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import de.javakaffee.kryoserializers.UUIDSerializer;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class EntityDataValueStreamSerializer implements SelfRegisteringStreamSerializer<EntityDataValue> {
    //Changing this or Kryo will affect how hash is computed for database.
    private static final  int               CHUNK_SIZE      = 32;
    private static final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial( () -> {
        Kryo kryo = new Kryo();
        kryo.register( UUID.class, new UUIDSerializer() );
        return kryo;
    } );

    @Override
    public void write( ObjectDataOutput out, EntityDataValue object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public EntityDataValue read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY_DATA_VALUE.ordinal();
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<EntityDataValue> getClazz() {
        return EntityDataValue.class;
    }

    public static void serialize( ObjectDataOutput out, EntityDataValue object ) throws IOException {
        serialize( out, object.getMetadata() );

        Map<UUID, Map<Object, PropertyMetadata>> propertiesMetadata = object.getProperties();

        out.writeInt( propertiesMetadata.size() );

        for ( Entry<UUID, Map<Object, PropertyMetadata>> propertiesEntry : propertiesMetadata.entrySet() ) {
            UUIDStreamSerializer.serialize( out, propertiesEntry.getKey() );
            serialize( out, propertiesEntry.getValue() );
        }
    }

    public static EntityDataValue deserialize( ObjectDataInput in ) throws IOException {
        final EntityDataMetadata entityMetadata = deserializeEntityMetadata( in );

        int propertyTypeCount = in.readInt();

        final Map<UUID, Map<Object, PropertyMetadata>> propertiesMetadata = new HashMap<>( propertyTypeCount );

        for ( int i = 0; i < propertyTypeCount; ++i ) {
            UUID propertyTypeId = UUIDStreamSerializer.deserialize( in );
            Map<Object, PropertyMetadata> pmm = deserializePropertyMetadata( in );
            propertiesMetadata.put( propertyTypeId, pmm );
        }
        return new EntityDataValue( entityMetadata, propertiesMetadata );
    }

    public static void serialize( ObjectDataOutput out, EntityDataMetadata metadata ) throws IOException {
        out.writeLong( metadata.getVersion() );
        OffsetDateTimeStreamSerializer.serialize( out, metadata.getLastWrite() );
        OffsetDateTimeStreamSerializer.serialize( out, metadata.getLastIndex() );
    }

    public static EntityDataMetadata deserializeEntityMetadata( ObjectDataInput in ) throws IOException {
        long version = in.readLong();
        OffsetDateTime lastWrite = OffsetDateTimeStreamSerializer.deserialize( in );
        OffsetDateTime lastIndex = OffsetDateTimeStreamSerializer.deserialize( in );
        return new EntityDataMetadata( version, lastWrite, lastIndex );
    }

    public static void serialize( ObjectDataOutput out, Map<Object, PropertyMetadata> properties ) throws IOException {
        out.writeInt( properties.size() );
        for ( Entry<Object, PropertyMetadata> property : properties.entrySet() ) {
            final Object value = property.getKey();
            final PropertyMetadata pm = property.getValue();
            final List<Long> versions = pm.getVersions();

            out.writeByteArray( pm.getHash() );
            Jdk8StreamSerializers.serializeWithKryo( kryoThreadLocal.get(), out, value, CHUNK_SIZE );

            OffsetDateTimeStreamSerializer.serialize( out, pm.getLastWrite() );
            out.writeLong( pm.getVersion() );
            out.writeInt( versions.size() );
            for ( long version : versions ) {
                out.writeLong( version );
            }
            //            out.writeObject( value );

        }
    }

    public static Map<Object, PropertyMetadata> deserializePropertyMetadata( ObjectDataInput in ) throws IOException {
        final int propertyCount = in.readInt();
        final Map<Object, PropertyMetadata> properties = new HashMap<>( propertyCount );
        for ( int i = 0; i < propertyCount; ++i ) {

            final byte[] hash = in.readByteArray();
            final Object value = Jdk8StreamSerializers.deserializeWithKryo( kryoThreadLocal.get(), in, CHUNK_SIZE );
            final OffsetDateTime lastWrite = OffsetDateTimeStreamSerializer.deserialize( in );
            final long version = in.readLong();
            final int versionCount = in.readInt();

            List<Long> versions = new ArrayList<>( versionCount );

            for ( int j = 0; j < versionCount; ++j ) {
                versions.add( j, in.readLong() );
            }

            properties.put( value, new PropertyMetadata( hash, version, versions, lastWrite ) );
        }
        return properties;
    }

}
