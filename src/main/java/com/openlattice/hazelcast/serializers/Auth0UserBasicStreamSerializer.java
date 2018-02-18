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
import com.openlattice.directory.pojo.Auth0UserBasic;
import java.io.IOException;
import java.util.Set;
import org.springframework.stereotype.Component;

@Component
public class Auth0UserBasicStreamSerializer implements SelfRegisteringStreamSerializer<Auth0UserBasic> {
    @Override public Class<? extends Auth0UserBasic> getClazz() {
        return Auth0UserBasic.class;
    }

    @Override public void write( ObjectDataOutput out, Auth0UserBasic object ) throws IOException {
        out.writeUTF( object.getUserId() );
        out.writeUTF( object.getEmail() );
        out.writeUTF( object.getNickname() );
        SetStreamSerializers.fastStringSetSerialize( out, object.getRoles() );
        SetStreamSerializers.fastStringSetSerialize( out, object.getOrganizations() );
    }

    @Override public Auth0UserBasic read( ObjectDataInput in ) throws IOException {
        String userId = in.readUTF();
        String email = in.readUTF();
        String nickname = in.readUTF();
        Set<String> roles = SetStreamSerializers.fastStringSetDeserialize( in );
        Set<String> organizations = SetStreamSerializers.fastStringSetDeserialize( in );

        return new Auth0UserBasic( userId, email, nickname, roles, organizations );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.AUTH0_USER_BASIC.ordinal();
    }

    @Override public void destroy() {

    }
}
