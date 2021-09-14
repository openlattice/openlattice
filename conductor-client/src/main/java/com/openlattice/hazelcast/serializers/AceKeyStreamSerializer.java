

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
import com.openlattice.authorization.AceKey;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.Principal;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.openlattice.mapstores.TestDataFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class AceKeyStreamSerializer implements TestableSelfRegisteringStreamSerializer<AceKey> {

    @Override
    public void write( ObjectDataOutput out, AceKey object )
            throws IOException {
        AclKeyStreamSerializer.serialize( out, object.getAclKey() );
        PrincipalStreamSerializer.serialize( out, object.getPrincipal() );
    }

    @Override
    public AceKey read( ObjectDataInput in ) throws IOException {
        AclKey key = AclKeyStreamSerializer.deserialize( in );
        Principal principal = PrincipalStreamSerializer.deserialize( in );
        return new AceKey( key, principal );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ACE_KEY.ordinal();
    }

    @Override
    public void destroy() {

    }

    @Override
    public Class<AceKey> getClazz() {
        return AceKey.class;
    }

    @Override public AceKey generateTestValue() {
        return new AceKey( TestDataFactory.aclKey(), TestDataFactory.userPrincipal() );
    }
}
