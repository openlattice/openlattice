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
import com.openlattice.linking.LinkingVertex;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class LinkingVertexStreamSerializer implements SelfRegisteringStreamSerializer<LinkingVertex> {

    @Override
    public void write( ObjectDataOutput out, LinkingVertex object ) throws IOException {
        out.writeDouble( object.getDiameter() );
        SetStreamSerializers.serialize( out, object.getEntityKeys(), ( UUID ek ) -> {
            UUIDStreamSerializer.serialize( out, ek );
        } );
    }

    @Override
    public LinkingVertex read( ObjectDataInput in ) throws IOException {
        double diameter = in.readDouble();
        Set<UUID> entityKeys = SetStreamSerializers.deserialize( in, UUIDStreamSerializer::deserialize );
        return new LinkingVertex( diameter, entityKeys );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.LINKING_VERTEX.ordinal();
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<? extends LinkingVertex> getClazz() {
        return LinkingVertex.class;
    }

}
