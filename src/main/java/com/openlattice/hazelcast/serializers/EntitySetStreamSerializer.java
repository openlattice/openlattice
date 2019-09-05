

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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.data.DataExpiration;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.set.EntitySetFlag;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

@Component
public class EntitySetStreamSerializer implements SelfRegisteringStreamSerializer<EntitySet> {

    @Override
    public void write( ObjectDataOutput out, EntitySet object ) throws IOException {
        serialize( out, object );
    }

    public static void serialize( ObjectDataOutput out, EntitySet object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getId() );
        UUIDStreamSerializer.serialize( out, object.getEntityTypeId() );
        out.writeUTF( object.getName() );
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getDescription() );
        SetStreamSerializers.serialize( out, object.getContacts(), ObjectDataOutput::writeUTF );
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getLinkedEntitySets() );
        UUIDStreamSerializer.serialize( out, object.getOrganizationId() );
        out.writeInt( object.getFlags().size() );
        for ( EntitySetFlag flag : object.getFlags() ) {
            out.writeUTF( flag.toString() );
        }

        var partitions = new int[ object.getPartitions().size() ];
        var index = 0;
        for ( var partition : object.getPartitions() ) {
            partitions[ index++ ] = partition;
        }

        out.writeIntArray( partitions );

        out.writeInt( object.getPartitionsVersion() );

        if ( object.getExpiration() != null ) {
            out.writeBoolean( true );
            DataExpirationStreamSerializer.serialize( out, object.getExpiration() );
        } else {
            out.writeBoolean( false );
        }
    }

    @Override
    public EntitySet read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    public static EntitySet deserialize( ObjectDataInput in ) throws IOException {
        UUID id = UUIDStreamSerializer.deserialize( in );
        UUID entityTypeId = UUIDStreamSerializer.deserialize( in );
        String name = in.readUTF();
        String title = in.readUTF();
        String description = in.readUTF();
        Set<String> contacts = SetStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Set<UUID> linkedEntitySets = SetStreamSerializers.fastUUIDSetDeserialize( in );
        UUID organizationId = UUIDStreamSerializer.deserialize( in );

        int numFlags = in.readInt();
        EnumSet<EntitySetFlag> flags = EnumSet.noneOf( EntitySetFlag.class );
        for ( int i = 0; i < numFlags; i++ ) {
            flags.add( EntitySetFlag.valueOf( in.readUTF() ) );
        }

        var partitionsArray = in.readIntArray();
        var partitions = new LinkedHashSet<Integer>( partitionsArray.length );

        for ( var p : partitionsArray ) {
            partitions.add( p );
        }

        int partitionsVersion = in.readInt();

        DataExpiration expiration;
        boolean hasExpiration = in.readBoolean();
        if ( hasExpiration ) {
            expiration = DataExpirationStreamSerializer.deserialize( in );
        } else {
            expiration = null;
        }

        EntitySet es = new EntitySet(
                id,
                entityTypeId,
                name,
                title,
                description,
                contacts,
                linkedEntitySets,
                organizationId,
                flags,
                partitions,
                partitionsVersion,
                expiration );

        return es;
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY_SET.ordinal();
    }

    @Override
    public void destroy() {

    }

    @Override
    public Class<EntitySet> getClazz() {
        return EntitySet.class;
    }

}
