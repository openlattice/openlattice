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
import com.kryptnostic.rhizome.hazelcast.serializers.ListStreamSerializers;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.Permission;
import com.openlattice.requests.Request;
import java.io.IOException;
import java.util.EnumSet;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class RequestStreamSerializer implements SelfRegisteringStreamSerializer<Request> {

    @Override
    public void write( ObjectDataOutput out, Request object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public Request read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.REQUEST.ordinal();
    }

    @Override
    public void destroy() {

    }

    @Override
    public Class<Request> getClazz() {
        return Request.class;
    }

    public static void serialize( ObjectDataOutput out, Request object ) throws IOException {
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getAclKey() );
        DelegatedPermissionEnumSetStreamSerializer.serialize( out, object.getPermissions() );
        out.writeUTF( object.getReason() );
    }

    public static Request deserialize( ObjectDataInput in ) throws IOException {
        UUID[] aclKey = ListStreamSerializers.fastUUIDArrayDeserialize( in );
        EnumSet<Permission> permissions = DelegatedPermissionEnumSetStreamSerializer.deserialize( in );
        String reason = in.readUTF();
        return new Request( new AclKey( aclKey ), permissions, reason );
    }
}
