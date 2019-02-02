

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
import com.openlattice.edm.EntitySet;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class EntitySetStreamSerializer implements SelfRegisteringStreamSerializer<EntitySet> {

    @Override
    public void write( ObjectDataOutput out, EntitySet object )
            throws IOException {
        UUIDStreamSerializer.serialize( out, object.getId() );
        UUIDStreamSerializer.serialize( out, object.getEntityTypeId() );
        out.writeUTF( object.getName() );
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getDescription() );
        SetStreamSerializers.serialize( out, object.getContacts(), ObjectDataOutput::writeUTF );
        out.writeBoolean( object.isLinking() );
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getLinkedEntitySets() );
        out.writeBoolean( object.isExternal() );

    }

    @Override
    public EntitySet read( ObjectDataInput in ) throws IOException {
        Optional<UUID> id = Optional.of(UUIDStreamSerializer.deserialize( in ));
        UUID entityTypeId = UUIDStreamSerializer.deserialize( in );
        String name = in.readUTF();
        String title = in.readUTF();
        Optional<String> description = Optional.of( in.readUTF() );
        Set<String> contacts = SetStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Optional<Boolean> linking = Optional.of( in.readBoolean() );
        Optional<Set<UUID>> linkedEntitySets = Optional.of( SetStreamSerializers.fastUUIDSetDeserialize( in ) );
        Optional<Boolean> external = Optional.of( in.readBoolean() );

        EntitySet es = new EntitySet(
                id,
                entityTypeId,
                name,
                title,
                description,
                contacts,
                linking,
                linkedEntitySets,
                external );

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
