

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
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.authorization.DelegatedPermissionEnumSet;
import com.openlattice.authorization.Permission;
import java.io.IOException;
import java.util.EnumSet;
import org.springframework.stereotype.Component;

@Component
public class DelegatedPermissionEnumSetStreamSerializer
        implements SelfRegisteringStreamSerializer<DelegatedPermissionEnumSet> {

    private static final Permission[] permissions = Permission.values();

    @Override
    public void write( ObjectDataOutput out, DelegatedPermissionEnumSet object ) throws IOException {
        serialize( out, object.unwrap() );
    }

    @Override
    public DelegatedPermissionEnumSet read( ObjectDataInput in ) throws IOException {
        return DelegatedPermissionEnumSet.wrap( deserialize( in ) );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.PERMISSION_SET.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<DelegatedPermissionEnumSet> getClazz() {
        return DelegatedPermissionEnumSet.class;
    }

    public static void serialize( ObjectDataOutput out, EnumSet<Permission> object ) throws IOException {
        out.writeInt( object.size() );
        for ( Permission permission : object ) {
            out.writeInt( permission.ordinal() );
        }
    }

    public static EnumSet<Permission> deserialize( ObjectDataInput in ) throws IOException {
        int size = in.readInt();
        EnumSet<Permission> set = EnumSet.noneOf( Permission.class );
        for ( int i = 0; i < size; ++i ) {
            set.add( permissions[ in.readInt() ] );
        }
        return set;
    }

}
