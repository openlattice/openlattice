

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
import com.openlattice.authorization.DelegatedPermissionEnumSet;
import com.openlattice.authorization.Permission;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.EnumSet;

@Component
public class DelegatedPermissionEnumSetStreamSerializer
        implements SelfRegisteringStreamSerializer<DelegatedPermissionEnumSet> {

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
        RhizomeUtils.Serializers.serializeEnumSet( out, Permission.class, object );
    }

    public static EnumSet<Permission> deserialize( ObjectDataInput in ) throws IOException {
        return RhizomeUtils.Serializers.deSerializeEnumSet( in, Permission.class );
    }

}
