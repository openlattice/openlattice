

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
import com.kryptnostic.rhizome.hazelcast.serializers.RhizomeUtils;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.authorization.Permission;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.openlattice.requests.PermissionsRequestDetails;
import com.openlattice.requests.RequestStatus;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;

@Component
public class PermissionsRequestDetailsStreamSerializer
        implements SelfRegisteringStreamSerializer<PermissionsRequestDetails> {

    private static final RequestStatus[] status = RequestStatus.values();

    @Override
    public void write( ObjectDataOutput out, PermissionsRequestDetails object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public PermissionsRequestDetails read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.PERMISSIONS_REQUEST_DETAILS.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<PermissionsRequestDetails> getClazz() {
        return PermissionsRequestDetails.class;
    }

    public static void serialize( ObjectDataOutput out, PermissionsRequestDetails object ) throws IOException {
        StreamSerializerUtils.serializeFromMap( out, object.getPermissions(),
                ( UUID key ) -> UUIDStreamSerializer.serialize( out, key ),
                ( EnumSet<Permission> permissions ) -> RhizomeUtils.Serializers.serializeEnumSet( out, Permission.class, permissions ) );
        out.writeInt( object.getStatus().ordinal() );
    }
    
    public static PermissionsRequestDetails deserialize( ObjectDataInput in ) throws IOException {
        Map<UUID, EnumSet<Permission>> permissions = StreamSerializerUtils.deserializeToMap( in,
                UUIDStreamSerializer::deserialize,
                ( objectDataInput ) -> RhizomeUtils.Serializers.deSerializeEnumSet( in, Permission.class ) );
        RequestStatus st = status[ in.readInt() ];
        return new PermissionsRequestDetails( permissions, st );
    }

}
