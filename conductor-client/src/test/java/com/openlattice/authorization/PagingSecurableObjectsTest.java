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

package com.openlattice.authorization;

import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.mapstores.TestDataFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

public class PagingSecurableObjectsTest extends HzAuthzTest {
    private static final Logger logger = LoggerFactory.getLogger( PagingSecurableObjectsTest.class );

    // Entity Set acl Keys
    protected static final AclKey key1 = new AclKey( UUID.randomUUID() );
    protected static final AclKey key2 = new AclKey( UUID.randomUUID() );
    protected static final AclKey key3 = new AclKey( UUID.randomUUID() );

    // User and roles
    protected static final Principal               u1                = TestDataFactory.userPrincipal();
    protected static final Principal               r1                = TestDataFactory.rolePrincipal();
    protected static final Principal               r2                = TestDataFactory.rolePrincipal();
    protected static final Principal               r3                = TestDataFactory.rolePrincipal();
    protected static final NavigableSet<Principal> currentPrincipals = new TreeSet<Principal>();

    @BeforeClass
    public static void init() {
        HzAuthzTest.init();
        currentPrincipals.add( u1 );
        currentPrincipals.add( r1 );
        currentPrincipals.add( r2 );
        currentPrincipals.add( r3 );

        hzAuthz.addPermission( key1, u1, EnumSet.allOf( Permission.class ) );
        hzAuthz.setSecurableObjectType( key1, SecurableObjectType.EntitySet );
        hzAuthz.addPermission( key2, r1, EnumSet.of( Permission.READ, Permission.WRITE ) );
        hzAuthz.setSecurableObjectType( key2, SecurableObjectType.EntitySet );
        hzAuthz.addPermission( key3, r2, EnumSet.of( Permission.READ ) );
        hzAuthz.setSecurableObjectType( key3, SecurableObjectType.EntitySet );
    }

    @Test
    public void testListSecurableObjects() {
        Set<AclKey> result = hzAuthz.getAuthorizedObjectsOfType(
                currentPrincipals,
                SecurableObjectType.EntitySet,
                EnumSet.of( Permission.READ )
        ).collect( Collectors.toSet() );

        Assert.assertEquals( 3, result.size() );
    }

    @Test
    public void testNoResults() {
        Set<AclKey> result = hzAuthz.getAuthorizedObjectsOfType( currentPrincipals,
                SecurableObjectType.Organization,
                EnumSet.of( Permission.READ )
        ).collect( Collectors.toSet() );

        Assert.assertEquals( 0, result.size() );
    }

}
