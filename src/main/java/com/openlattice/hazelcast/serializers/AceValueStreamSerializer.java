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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.authorization.AceValue;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.securable.SecurableObjectType;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.EnumSet;
import java.util.Optional;

import org.springframework.stereotype.Component;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
public class AceValueStreamSerializer implements SelfRegisteringStreamSerializer<AceValue> {
    private static final SecurableObjectType[] lookup = SecurableObjectType.values();

    @Override
    public void write( ObjectDataOutput out, AceValue object ) throws IOException {
        DelegatedPermissionEnumSetStreamSerializer.serialize( out, object.getPermissions() );
        serialize( out, object.getSecurableObjectType() );
        OffsetDateTimeStreamSerializer.serialize(out, object.getExpirationDate());
    }

    @Override
    public AceValue read( ObjectDataInput in ) throws IOException {
        EnumSet<Permission> permissions = DelegatedPermissionEnumSetStreamSerializer.deserialize( in );
        SecurableObjectType objectType = deserialize( in );
        OffsetDateTime expirationDate = OffsetDateTimeStreamSerializer.deserialize( in );
        return new AceValue( permissions, objectType, expirationDate );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ACE_VALUE.ordinal();
    }

    @Override public void destroy() {

    }

    @Override public Class<AceValue> getClazz() {
        return AceValue.class;
    }

    public static void serialize( ObjectDataOutput out, SecurableObjectType object ) throws IOException {
        out.writeInt( object.ordinal() );
    }

    public static SecurableObjectType deserialize( ObjectDataInput in ) throws IOException {
        return lookup[ in.readInt() ];
    }
}
