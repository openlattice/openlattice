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

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.openlattice.TestServer;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.organizations.PrincipalSet;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HzAuthzTest extends TestServer {
    private static final Logger                        logger = LoggerFactory.getLogger( HzAuthzTest.class );
    protected static     HazelcastAuthorizationService hzAuthz;

    @Test
    public void testAddEntitySetPermission() {
        AclKey key = new AclKey( UUID.randomUUID() );
        Principal p = new Principal( PrincipalType.USER, "grid|TRON" );
        EnumSet<Permission> permissions = EnumSet.of( Permission.DISCOVER, Permission.READ );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p ), permissions ) );
        hzAuthz.addPermission( key, p, permissions );
        Assert.assertTrue(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p ), permissions ) );
    }

    @Test
    public void testAddEntitySetsPermissions() {
        Set<AclKey> aclKeys = ImmutableSet.of( new AclKey( UUID.randomUUID() ),
                new AclKey( UUID.randomUUID() ),
                new AclKey( UUID.randomUUID() ) );

        Principal p = new Principal( PrincipalType.USER, "grid|TRON" );
        EnumSet<Permission> permissions = EnumSet.of( Permission.DISCOVER, Permission.READ );
        aclKeys.forEach( key ->
                Assert.assertFalse(
                        hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p ), permissions ) ) );
        hzAuthz.addPermissions( aclKeys, p, permissions, SecurableObjectType.EntitySet );
        aclKeys.forEach( key ->
                Assert.assertTrue(
                        hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p ), permissions ) ) );
    }

    @Test
    public void testTypeMistmatchPermission() {
        AclKey key = new AclKey( UUID.randomUUID() );
        Principal p = new Principal( PrincipalType.USER, "grid|TRON" );
        EnumSet<Permission> permissions = EnumSet.of( Permission.DISCOVER, Permission.READ );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p ), permissions ) );
        hzAuthz.setSecurableObjectType( key, SecurableObjectType.EntitySet );
        hzAuthz.addPermission( key, p, permissions );
        UUID badkey = UUID.randomUUID();
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( new AclKey( badkey ), ImmutableSet.of( p ), permissions ) );
    }

    @Test
    public void testRemovePermissions() {
        AclKey key = new AclKey( UUID.randomUUID() );
        Principal p = new Principal( PrincipalType.USER, "grid|TRON" );
        Principal p2 = new Principal( PrincipalType.USER, "grid|TRON2" );
        EnumSet<Permission> permissions = EnumSet.of( Permission.DISCOVER, Permission.READ, Permission.OWNER );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p ), permissions ) );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p2 ), permissions ) );
        hzAuthz.setSecurableObjectType( key, SecurableObjectType.EntitySet );
        hzAuthz.addPermission( key, p, permissions );
        hzAuthz.addPermission( key, p2, permissions );
        Assert.assertTrue(
                hzAuthz.checkIfHasPermissions( new AclKey( key ), ImmutableSet.of( p ), permissions ) );
        Assert.assertTrue(
                hzAuthz.checkIfHasPermissions( new AclKey( key ), ImmutableSet.of( p2 ), permissions ) );
        hzAuthz.removePermission( key, p, permissions );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p ), permissions ) );
    }

    @Test
    public void testSetPermissions() {
        AclKey key = new AclKey( UUID.randomUUID() );
        Principal p = new Principal( PrincipalType.USER, "grid|TRON" );
        Principal p2 = new Principal( PrincipalType.USER, "grid|TRON2" );
        EnumSet<Permission> permissions = EnumSet.of( Permission.DISCOVER, Permission.READ, Permission.OWNER );
        EnumSet<Permission> badPermissions = EnumSet.of( Permission.DISCOVER, Permission.READ, Permission.LINK );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p ), permissions ) );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p2 ), permissions ) );

        hzAuthz.setSecurableObjectType( key, SecurableObjectType.EntitySet );
        hzAuthz.setPermission( key, p, permissions );
        hzAuthz.setPermission( key, p2, permissions );

        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p ), badPermissions ) );
        Assert.assertTrue(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p ), permissions ) );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p2 ), badPermissions ) );
        Assert.assertTrue(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p2 ), permissions ) );
    }

    @Test
    public void testListSecurableObjects() {
        AclKey key = new AclKey( UUID.randomUUID() );
        Principal p1 = TestDataFactory.userPrincipal();
        Principal p2 = TestDataFactory.userPrincipal();

        EnumSet<Permission> permissions1 = EnumSet.of( Permission.DISCOVER, Permission.READ );
        EnumSet<Permission> permissions2 = EnumSet
                .of( Permission.DISCOVER, Permission.READ, Permission.WRITE, Permission.OWNER );

        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p1 ), permissions1 ) );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p2 ), permissions2 ) );

        hzAuthz.setSecurableObjectType( key, SecurableObjectType.EntitySet );
        hzAuthz.addPermission( key, p1, permissions1 );
        hzAuthz.addPermission( key, p2, permissions2 );

        Assert.assertTrue(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p1 ), permissions1 ) );

        Assert.assertFalse( hzAuthz.checkIfHasPermissions( key,
                ImmutableSet.of( p1 ),
                EnumSet.of( Permission.WRITE, Permission.OWNER ) ) );

        Assert.assertTrue(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p2 ), permissions2 ) );

        Stream<AclKey> p1Owned = hzAuthz.getAuthorizedObjectsOfType( ImmutableSet.of( p1 ),
                SecurableObjectType.EntitySet,
                EnumSet.of( Permission.OWNER ) );

        Set<List<UUID>> p1s = p1Owned.collect( Collectors.toSet() );

        if ( p1s.size() > 0 ) {
            Set<Permission> permissions = hzAuthz.getSecurableObjectPermissions( key, ImmutableSet.of( p1 ) );
            Assert.assertTrue( permissions.contains( Permission.OWNER ) );
            Assert.assertTrue(
                    hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p1 ), EnumSet.of( Permission.OWNER ) ) );
        }

        Stream<AclKey> p2Owned = hzAuthz.getAuthorizedObjectsOfType( ImmutableSet.of( p2 ),
                SecurableObjectType.EntitySet,
                EnumSet.of( Permission.OWNER ) );

        Set<List<UUID>> p2s = p2Owned.collect( Collectors.toSet() );
        Assert.assertTrue( p1s.isEmpty() );
        Assert.assertEquals( 1, p2s.size() );
        Assert.assertFalse( p1s.contains( key ) );
        Assert.assertTrue( p2s.contains( key ) );
    }

    @Test
    public void testAccessChecks() {
        final int size = 100;
        Set<AccessCheck> accessChecks = new HashSet<>( size );
        AclKey[] aclKeys = new AclKey[ size ];
        //        Principal[] p1s = new Principal[ size ];
        Principal[] p2s = new Principal[ size ];
        EnumSet<Permission>[] permissions1s = new EnumSet[ size ];
        EnumSet<Permission>[] permissions2s = new EnumSet[ size ];
        EnumSet<Permission> all = EnumSet.noneOf( Permission.class );

        for ( int i = 0; i < size; ++i ) {
            AclKey key = new AclKey( UUID.randomUUID() );
            Principal p1 = TestDataFactory.userPrincipal();
            Principal p2 = TestDataFactory.userPrincipal();
            aclKeys[ i ] = key;
            //            p1s[ i ] = p1;
            p2s[ i ] = p2;

            EnumSet<Permission> permissions1 = permissions1s[ i ] = TestDataFactory.nonEmptyPermissions();
            EnumSet<Permission> permissions2 = permissions2s[ i ] = TestDataFactory.nonEmptyPermissions();

            all.addAll( permissions2 );

            Assert.assertFalse(
                    hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p1 ), permissions1 ) );
            Assert.assertFalse(
                    hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p2 ), permissions2 ) );

            hzAuthz.setSecurableObjectType( key, SecurableObjectType.EntitySet );
            hzAuthz.addPermission( key, p1, permissions1 );
            hzAuthz.addPermission( key, p2, permissions2 );

            Assert.assertTrue( hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p1 ), permissions1 ) );

            Assert.assertEquals( permissions1.containsAll( permissions2 ),
                    hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p1 ), permissions2 ) );

            Assert.assertTrue( hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p2 ), permissions2 ) );
            AccessCheck ac = new AccessCheck( key, permissions1 );
            accessChecks.add( ac );
        }
        int i = 0;
        List<AclKey> keys = Arrays.asList( aclKeys );
        for ( AccessCheck ac : accessChecks ) {
            AclKey key = ac.getAclKey();
            //            Principal p1 = p1s[ i ];
            i = keys.indexOf( key );
            Principal p2 = p2s[ i ];
            EnumSet<Permission> permissions1 = permissions1s[ i ];
            EnumSet<Permission> permissions2 = permissions2s[ i++ ];

            Map<AclKey, EnumMap<Permission, Boolean>> result =
                    hzAuthz.accessChecksForPrincipals( ImmutableSet.of( ac ), ImmutableSet.of( p2 ) )
                            .collect( Collectors.toConcurrentMap( a -> a.getAclKey(),
                                    a -> new EnumMap<>( a.getPermissions() ) ) );

            Assert.assertTrue( result.containsKey( key ) );
            EnumMap<Permission, Boolean> checkForKey = result.get( key );
            Assert.assertTrue( checkForKey.size() == ac.getPermissions().size() );
            Assert.assertTrue( checkForKey.keySet().containsAll( ac.getPermissions() ) );
            Set<Permission> overlapping = ImmutableSet.copyOf( Sets.intersection( permissions2, ac.getPermissions() ) );
            Assert.assertTrue( overlapping.stream().allMatch( result.get( key )::get ) );
            //            Assert.assertTrue( result.get( key ).get( Permission.DISCOVER ) );
            //            Assert.assertTrue( result.get( key ).get( Permission.READ ) );
            //            Assert.assertFalse( result.get( key ).get( Permission.OWNER ) );
        }
        Stopwatch w = Stopwatch.createStarted();
        Map<AclKey, EnumMap<Permission, Boolean>> result =
                hzAuthz.accessChecksForPrincipals( accessChecks, ImmutableSet.copyOf( p2s ) )
                        .collect( Collectors.toConcurrentMap( a -> a.getAclKey(),
                                a -> new EnumMap<>( a.getPermissions() ) ) );
        logger.info( "Elapsed time to access check: {} ms", w.elapsed( TimeUnit.MILLISECONDS ) );
        Assert.assertTrue( result.keySet().containsAll( Arrays.asList( aclKeys ) ) );

    }

    @Test
    public void testGetAuthorizedPrincipalsOnSecurableObject() {
        AclKey key = new AclKey( UUID.randomUUID() );
        Principal p1 = TestDataFactory.userPrincipal();
        Principal p2 = TestDataFactory.userPrincipal();
        Principal p3 = TestDataFactory.userPrincipal();

        EnumSet<Permission> permissions = EnumSet.of( Permission.READ );
        hzAuthz.addPermission( key, p1, permissions );
        hzAuthz.addPermission( key, p2, permissions );
        PrincipalSet authorizedPrincipals = new PrincipalSet(
                hzAuthz.getAuthorizedPrincipalsOnSecurableObject( key, permissions ) );

        Assert.assertEquals( Set.of( p1, p2 ), authorizedPrincipals );
    }

    @Test
    public void testGetSecurableObjectSetsPermissions() {
        var key1 = new AclKey( UUID.randomUUID() );
        var key2 = new AclKey( UUID.randomUUID() );
        var key3 = new AclKey( UUID.randomUUID() );
        var key4 = new AclKey( UUID.randomUUID() );
        var key5 = new AclKey( UUID.randomUUID() );
        var key6 = new AclKey( UUID.randomUUID() );

        Principal principal = TestDataFactory.userPrincipal();
        EnumSet<Permission> read = EnumSet.of( Permission.READ );
        EnumSet<Permission> write = EnumSet.of( Permission.WRITE );
        EnumSet<Permission> owner = EnumSet.of( Permission.OWNER );
        EnumSet<Permission> materialize = EnumSet.of( Permission.MATERIALIZE );
        EnumSet<Permission> discover = EnumSet.of( Permission.DISCOVER );

        // has read for all 3 acls, owner for 2, write for 2
        var aclKeySet1 = Set.of( key1, key2, key3 );

        hzAuthz.addPermission( key1, principal, read );
        hzAuthz.addPermission( key2, principal, read );
        hzAuthz.addPermission( key3, principal, read );

        hzAuthz.addPermission( key1, principal, write );
        hzAuthz.addPermission( key2, principal, write );

        hzAuthz.addPermission( key2, principal, owner );
        hzAuthz.addPermission( key3, principal, owner );

        // has all 3 on one, none on other
        var aclKeySet2 = Set.of( key4, key5 );

        hzAuthz.addPermission( key4, principal, materialize );
        hzAuthz.addPermission( key4, principal, discover );

        // no permissions at all
        var aclKeySet3 = Set.of( key5, key6 );

        final var reducedPermissionsMap1 = hzAuthz.getSecurableObjectSetsPermissions(
                List.of( aclKeySet1, aclKeySet2, aclKeySet3 ),
                Set.of( principal ) );

        Assert.assertEquals( read, reducedPermissionsMap1.get( aclKeySet1 ) );
        Assert.assertEquals( EnumSet.noneOf( Permission.class ), reducedPermissionsMap1.get( aclKeySet2 ) );
        Assert.assertEquals( EnumSet.noneOf( Permission.class ), reducedPermissionsMap1.get( aclKeySet3 ) );

        // different principals permissions should accumulate toghether
        Principal p1 = TestDataFactory.userPrincipal();
        Principal p2 = TestDataFactory.userPrincipal();
        Principal p3 = TestDataFactory.userPrincipal();

        hzAuthz.addPermission( key1, p1, read );
        hzAuthz.addPermission( key1, p1, write );
        hzAuthz.addPermission( key2, p2, read );
        hzAuthz.addPermission( key2, p2, owner );
        hzAuthz.addPermission( key3, p3, read );
        hzAuthz.addPermission( key3, p3, materialize );

        final var reducedPermissionsMap2 = hzAuthz.getSecurableObjectSetsPermissions(
                List.of( aclKeySet1 ),
                Set.of( p1, p2, p3 ) );
        Assert.assertEquals( read, reducedPermissionsMap2.get( aclKeySet1 ) );
    }

    @BeforeClass
    public static void init() {
        hzAuthz = new HazelcastAuthorizationService(
                hazelcastInstance,
                testServer.getContext().getBean( EventBus.class )
        );
    }

}
