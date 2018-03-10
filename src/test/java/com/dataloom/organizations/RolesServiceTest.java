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

package com.dataloom.organizations;

import com.openlattice.client.RetrofitFactory;
import com.openlattice.directory.pojo.Auth0UserBasic;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.organization.OrganizationsApi;
import com.openlattice.organization.roles.Role;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.openlattice.authentication.AuthenticationTest;
import com.openlattice.authentication.AuthenticationTestRequestOptions;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

import java.util.*;

public class RolesServiceTest extends OrganizationsTest {
    protected static final Map<String, AuthenticationTestRequestOptions> authOptionsMap = new HashMap<>();
    protected static final Map<String, Retrofit>                         retrofitMap    = new HashMap<>();
    private static final Logger logger = LoggerFactory
            .getLogger( RolesServiceTest.class );
    private static UUID organizationId;

    static {
        authOptionsMap.put( "user1", authOptions1 );
        authOptionsMap.put( "user2", authOptions2 );
        authOptionsMap.put( "user3", authOptions3 );
        retrofitMap.put( "user1", retrofit1 );
        retrofitMap.put( "user2", retrofit2 );
        retrofitMap.put( "user3", retrofit3 );
    }

    @BeforeClass
    public static void init() {
        organizationId = organizations.createOrganizationIfNotExists( TestDataFactory.organization() );
    }

    /**
     * Utils
     */
    protected static OrganizationsApi refreshOrganizationApi( String user ) {
        AuthenticationTestRequestOptions authOption = authOptionsMap.get( user );
        if ( authOption == null ) {
            throw new IllegalArgumentException( "User does not exists in Retrofit map." );
        }
        String jwt = (String) AuthenticationTest.refreshAndGetAuthentication( authOptionsMap.get( user ) ).getCredentials();
        Retrofit r = RetrofitFactory.newClient( RetrofitFactory.Environment.TESTING, () -> jwt );

        return r.create( OrganizationsApi.class );
    }

    private Role createRole( UUID organizationId ) {
        Role role = TestDataFactory.role( organizationId );
        UUID roleId = organizations.createRole( role );
        Assert.assertNotNull( roleId );
        Role registered = organizations.getRole( organizationId, roleId );
        Assert.assertNotNull( registered );
        return registered;
    }

    @Test
    public void testCreateRole() {
        createRole( organizationId );
    }

    @Test
    public void testGetRoles() {
        int initialNumRoles = Iterables.size( organizations.getRoles( organizationId ) );

        Role newRole = createRole( organizationId );

        Set<Role> allRoles = ImmutableSet.copyOf( organizations.getRoles( organizationId ) );

        Assert.assertTrue( allRoles.contains( newRole ) );
        Assert.assertEquals( initialNumRoles + 1, allRoles.size() );
    }

    @Test
    public void testUpdateRoleTitle() {
        Role newRole = createRole( organizationId );

        String newTitle = RandomStringUtils.randomAlphanumeric( 5 );
        organizations.updateRoleTitle( organizationId, newRole.getId(), newTitle );

        Assert.assertEquals( newTitle, organizations.getRole( organizationId, newRole.getId() ).getTitle() );
    }

    @Test
    public void testUpdateRoleDescription() {
        Role newRole = createRole( organizationId );

        String newDescription = RandomStringUtils.randomAlphanumeric( 5 );
        organizations.updateRoleDescription( organizationId, newRole.getId(), newDescription );

        Assert.assertEquals( newDescription,
                organizations.getRole( organizationId, newRole.getId() ).getDescription() );
    }

    @Test
    public void testDeleteRole() {
        Role newRole = createRole( organizationId );

        organizations.deleteRole( organizationId, newRole.getId() );

        Assert.assertFalse( Iterables.contains( organizations.getRoles( organizationId ), newRole ) );
    }

    //TODO: Re-enable after adding the ability to get management token for the current configuration.
    @Test
    @Ignore
    public void testAddRemoveRoleToUser() {
        Role newRole = createRole( organizationId );

        organizations.addRoleToUser( organizationId, newRole.getId(), user2.getId() );

        Iterable<Auth0UserBasic> users = organizations.getAllUsersOfRole( organizationId, newRole.getId() );
        Assert.assertNotNull( "All users of roll cannot be null", users );
        Iterable<String> usersOfRoleAfterAdding = Iterables.transform( users, Auth0UserBasic::getUserId );
        Assert.assertTrue( Iterables.contains( usersOfRoleAfterAdding, user2.getId() ) );

        organizations.removeRoleFromUser( organizationId, newRole.getId(), user2.getId() );

        Iterable<Auth0UserBasic> usersOfRoleAfterRemovingInFull = organizations
                .getAllUsersOfRole( organizationId, newRole.getId() );
        if ( usersOfRoleAfterRemovingInFull == null ) {
            usersOfRoleAfterRemovingInFull = new ArrayList<Auth0UserBasic>();
        }
        Iterable<String> usersOfRoleAfterRemoving = Iterables.transform(
                usersOfRoleAfterRemovingInFull, Auth0UserBasic::getUserId );
        Assert.assertFalse( Iterables.contains( usersOfRoleAfterRemoving, user2.getId() ) );
    }

    // TODO: Temporarily turn off manual token expiration
    @Ignore
    public void testRefreshToken() {
        // add role to user2
        Role newRole = createRole( organizationId );
        organizations.addRoleToUser( organizationId, newRole.getId(), user2.getId() );

        Iterable<String> usersOfRoleAfterAdding = Iterables.transform(
                organizations.getAllUsersOfRole( organizationId, newRole.getId() ), Auth0UserBasic::getUserId );
        Assert.assertTrue( Iterables.contains( usersOfRoleAfterAdding, user2.getId() ) );

        logger.info( "Login as user2, anticipating a failed call with TokenRefreshException." );
        OrganizationsApi orgsApiOfUser2 = retrofit2.create( OrganizationsApi.class );
        // This call should fail with TokenRefreshException
        Assert.assertNull( orgsApiOfUser2.createOrganizationIfNotExists( TestDataFactory.organization() ) );

        logger.info( "Refreshed Login as user2, anticipating a success call." );
        // Update token
        OrganizationsApi refreshedOrgsApiOfUser2 = refreshOrganizationApi( "user2" );
        // This call should now succeed
        Assert.assertNotNull( refreshedOrgsApiOfUser2.createOrganizationIfNotExists( TestDataFactory.organization() ) );
    }
}
