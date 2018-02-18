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
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultArraySerializers.ByteArraySerializer;
import com.google.common.collect.SetMultimap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.data.EntityKey;
import com.openlattice.data.storage.EntityBytes;
import de.javakaffee.kryoserializers.UUIDSerializer;
import de.javakaffee.kryoserializers.guava.HashMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMultimapSerializer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class EntityBytesStreamSerializer implements SelfRegisteringStreamSerializer<EntityBytes> {

    private static final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial( () -> {
        Kryo kryo = new Kryo();
        kryo.register( UUID.class, new UUIDSerializer() );
        kryo.register( byte[].class, new ByteArraySerializer() );
        HashMultimapSerializer.registerSerializers( kryo );
        ImmutableMultimapSerializer.registerSerializers( kryo );
        return kryo;
    } );

    @Override
    public void write( ObjectDataOutput out, EntityBytes object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public EntityBytes read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY_BYTES.ordinal();
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<EntityBytes> getClazz() {
        return EntityBytes.class;
    }

    @SuppressFBWarnings
    public static void serialize( ObjectDataOutput out, EntityBytes object ) throws IOException {
        EntityKeyStreamSerializer.serialize( out, object.getKey() );
        Output output = new Output( (OutputStream) out );
        kryoThreadLocal.get().writeClassAndObject( output, object.getRaw() );
        output.flush();
    }

    @SuppressFBWarnings
    public static EntityBytes deserialize( ObjectDataInput in ) throws IOException {
        EntityKey ek = EntityKeyStreamSerializer.deserialize( in );
        Input input = new Input( (InputStream) in );
        SetMultimap<UUID, byte[]> m = (SetMultimap<UUID, byte[]>) kryoThreadLocal.get().readClassAndObject( input );
        return new EntityBytes( ek, m );
    }

}
