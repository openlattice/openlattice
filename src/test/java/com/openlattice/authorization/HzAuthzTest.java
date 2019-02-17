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

import com.openlattice.auditing.pods.AuditingConfigurationPod;
import com.openlattice.datastore.constants.DatastoreProfiles;
import com.openlattice.hazelcast.pods.MapstoresPod;
import com.openlattice.hazelcast.pods.SharedStreamSerializersPod;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.rhizome.configuration.ConfigurationConstants;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.openlattice.auth0.Auth0Pod;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.edm.PostgresEdmManager;
import com.openlattice.jdbc.JdbcPod;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.organizations.PrincipalSet;
import com.openlattice.postgres.PostgresPod;
import com.openlattice.postgres.PostgresTableManager;
import com.openlattice.postgres.PostgresTablesPod;
import com.zaxxer.hikari.HikariDataSource;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HzAuthzTest {
    protected static final RhizomeApplicationServer      testServer;
    protected static final HazelcastInstance             hazelcastInstance;
    protected static final AuthorizationQueryService     aqs;
    protected static final HazelcastAuthorizationService hzAuthz;
    protected static final HikariDataSource              hds;
    private static final   Logger                        logger = LoggerFactory.getLogger( HzAuthzTest.class );

    static {
        testServer = new RhizomeApplicationServer(
                Auth0Pod.class,
                MapstoresPod.class,
                JdbcPod.class,
                PostgresPod.class,
                SharedStreamSerializersPod.class,
                PostgresTablesPod.class,
                AuditingConfigurationPod.class
        );

        testServer.sprout( ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE, PostgresPod.PROFILE,
                DatastoreProfiles.MEDIA_LOCAL_PROFILE );
        hazelcastInstance = testServer.getContext().getBean( HazelcastInstance.class );

        hds = testServer.getContext().getBean( HikariDataSource.class );

        aqs = new AuthorizationQueryService( hds, hazelcastInstance );
        hzAuthz = new HazelcastAuthorizationService(
                hazelcastInstance,
                aqs,
                testServer.getContext().getBean( EventBus.class )
        );
        final var tableManager = testServer.getContext().getBean( PostgresTableManager.class );
        testServer.getContext().getBean( EventBus.class )
                .register( new PostgresEdmManager( hds, tableManager, hazelcastInstance ) );
    }

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
        EnumSet<Permission> permissions = EnumSet.of( Permission.DISCOVER, Permission.READ );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p ), permissions ) );
        hzAuthz.setSecurableObjectType( key, SecurableObjectType.EntitySet );
        hzAuthz.addPermission( key, p, permissions );
        Assert.assertTrue(
                hzAuthz.checkIfHasPermissions( new AclKey( key ), ImmutableSet.of( p ), permissions ) );
        hzAuthz.removePermission( key, p, permissions );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p ), permissions ) );
    }

    @Test
    public void testSetPermissions() {
        AclKey key = new AclKey( UUID.randomUUID() );
        Principal p = new Principal( PrincipalType.USER, "grid|TRON" );
        EnumSet<Permission> permissions = EnumSet.of( Permission.DISCOVER, Permission.READ );
        EnumSet<Permission> badPermissions = EnumSet.of( Permission.DISCOVER, Permission.READ, Permission.LINK );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p ), permissions ) );

        hzAuthz.setSecurableObjectType( key, SecurableObjectType.EntitySet );
        hzAuthz.setPermission( key, p, permissions );

        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p ), badPermissions ) );
        Assert.assertTrue(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p ), permissions ) );
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

}
