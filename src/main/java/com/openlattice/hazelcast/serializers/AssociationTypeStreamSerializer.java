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
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class AssociationTypeStreamSerializer implements SelfRegisteringStreamSerializer<AssociationType> {

    @Override
    public void write( ObjectDataOutput out, AssociationType object ) throws IOException {
        SetStreamSerializers.serialize( out, object.getSrc(), ( UUID key ) -> {
            UUIDStreamSerializer.serialize( out, key );
        } );
        SetStreamSerializers.serialize( out, object.getDst(), ( UUID property ) -> {
            UUIDStreamSerializer.serialize( out, property );
        } );
        out.writeBoolean( object.isBidirectional() );

        EntityType maybeEntityType = object.getAssociationEntityType();
        if ( maybeEntityType != null ) {
            out.writeBoolean( true );
            EntityTypeStreamSerializer.serialize( out, maybeEntityType );
        } else {
            out.writeBoolean( false );
        }
    }

    @Override
    public AssociationType read( ObjectDataInput in ) throws IOException {
        LinkedHashSet<UUID> src = SetStreamSerializers.orderedDeserialize( in, UUIDStreamSerializer::deserialize );
        LinkedHashSet<UUID> dst = SetStreamSerializers.orderedDeserialize( in, UUIDStreamSerializer::deserialize );
        boolean bidirectional = in.readBoolean();
        Optional<EntityType> entityType = Optional.empty();
        if ( in.readBoolean() ) entityType = Optional.of( EntityTypeStreamSerializer.deserialize( in ) );

        return new AssociationType( entityType, src, dst, bidirectional );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ASSOCIATION_TYPE.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<? extends AssociationType> getClazz() {
        return AssociationType.class;
    }

}
