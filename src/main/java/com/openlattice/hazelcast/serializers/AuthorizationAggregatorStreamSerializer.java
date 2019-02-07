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

import com.codahale.metrics.annotation.Timed;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.aggregators.AuthorizationAggregator;
import com.openlattice.authorization.Permission;
import java.io.IOException;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.springframework.stereotype.Component;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
public class AuthorizationAggregatorStreamSerializer
        implements SelfRegisteringStreamSerializer<AuthorizationAggregator> {
    @Override public Class<AuthorizationAggregator> getClazz() {
        return AuthorizationAggregator.class;
    }

    @Timed
    @Override
    public void write(
            ObjectDataOutput out, AuthorizationAggregator object ) throws IOException {
        Map<AclKey, EnumMap<Permission, Boolean>> permissionMap = object.getPermissions();
        out.writeInt( permissionMap.size() );
        for ( Entry<AclKey, EnumMap<Permission, Boolean>> permissionEntry : permissionMap.entrySet() ) {
            AclKeyStreamSerializer.serialize( out, permissionEntry.getKey() );
            serializePermissionEntry( out, permissionEntry.getValue() );
        }
    }

    @Timed
    @Override public AuthorizationAggregator read( ObjectDataInput in ) throws IOException {
        int size = in.readInt();
        Map<AclKey, EnumMap<Permission, Boolean>> permissionMap = new HashMap<>( size );
        for ( int i = 0; i < size; ++i ) {
            AclKey aclKey = AclKeyStreamSerializer.deserialize( in );
            EnumMap<Permission, Boolean> permissions = deserializePermissionEntry( in );
            permissionMap.put( aclKey, permissions );
        }
        return new AuthorizationAggregator( permissionMap );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.AUTHORIZATION_AGGREGATOR.ordinal();
    }

    @Override public void destroy() {

    }

    public static void serializePermissionEntry( ObjectDataOutput out, EnumMap<Permission, Boolean> object )
            throws IOException {
        PermissionMergerStreamSerializer
                .serialize( out, object.entrySet().stream().filter( e -> e.getValue() ).map(
                        Entry::getKey )::iterator );
        PermissionMergerStreamSerializer
                .serialize( out, object.entrySet().stream().filter( e -> !e.getValue() ).map(
                        Entry::getKey )::iterator );

    }

    public static EnumMap<Permission, Boolean> deserializePermissionEntry( ObjectDataInput in ) throws IOException {
        EnumMap<Permission, Boolean> pMap = new EnumMap<>( Permission.class );
        EnumSet<Permission> truePermissions = PermissionMergerStreamSerializer.deserialize( in );
        EnumSet<Permission> falsePermissions = PermissionMergerStreamSerializer.deserialize( in );
        for ( Permission p : truePermissions ) {
            pMap.put( p, true );
        }

        for ( Permission p : falsePermissions ) {
            pMap.put( p, false );
        }

        return pMap;
    }
}
