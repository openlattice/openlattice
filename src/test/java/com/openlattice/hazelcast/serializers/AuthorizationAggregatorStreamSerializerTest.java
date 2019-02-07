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

import com.openlattice.authorization.Permission;
import com.openlattice.mapstores.TestDataFactory;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.aggregators.AuthorizationAggregator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class AuthorizationAggregatorStreamSerializerTest
        extends AbstractStreamSerializerTest<AuthorizationAggregatorStreamSerializer, AuthorizationAggregator> {
    private static Random       r           = new Random();
    private static Permission[] permissions = Permission.values();

    @Override protected AuthorizationAggregatorStreamSerializer createSerializer() {
        return new AuthorizationAggregatorStreamSerializer();
    }

    @Override protected AuthorizationAggregator createInput() {
        Map<AclKey, EnumMap<Permission, Boolean>> permissionsMap = new HashMap<>();

        permissionsMap.put( TestDataFactory.aclKey(), createPermissionEntry() );
        permissionsMap.put( TestDataFactory.aclKey(), createPermissionEntry() );
        permissionsMap.put( TestDataFactory.aclKey(), createPermissionEntry() );
        permissionsMap.put( TestDataFactory.aclKey(), createPermissionEntry() );
        permissionsMap.put( TestDataFactory.aclKey(), createPermissionEntry() );
        return new AuthorizationAggregator( permissionsMap );
    }

    private static EnumMap<Permission, Boolean> createPermissionEntry() {
        EnumMap<Permission, Boolean> pmap = new EnumMap<>( Permission.class );
        int count = r.nextInt( permissions.length );
        for ( int i = 0; i < count; ++i ) {
            pmap.put( permissions[ r.nextInt( permissions.length ) ], r.nextBoolean() );
        }
        return pmap;
    }
}
