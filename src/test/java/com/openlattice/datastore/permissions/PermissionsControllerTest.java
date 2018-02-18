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

package com.openlattice.datastore.permissions;

import com.google.common.collect.Iterables;
import java.util.EnumSet;
import java.util.UUID;

import com.openlattice.authorization.AclKey;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.openlattice.authorization.Ace;
import com.openlattice.authorization.AceExplanation;
import com.openlattice.authorization.Acl;
import com.openlattice.authorization.AclData;
import com.openlattice.authorization.AclExplanation;
import com.openlattice.authorization.Action;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.datastore.authentication.MultipleAuthenticatedUsersBase;
import com.openlattice.edm.EntitySet;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.organization.roles.Role;
import com.google.common.collect.ImmutableSet;

public class PermissionsControllerTest extends MultipleAuthenticatedUsersBase {
    protected static AclKey entitySetAclKey;
    protected static Principal  rolePrincipal1;
    protected static Principal  rolePrincipal2;

    @BeforeClass
    public static void init() {
        loginAs( "admin" );
        EntitySet es = createEntitySet();
        entitySetAclKey = new AclKey( es.getId() );

        // create some roles
        UUID organizationId = organizationsApi.createOrganizationIfNotExists( TestDataFactory.organization() );
        Role r1 = TestDataFactory.role( organizationId );
        Role r2 = TestDataFactory.role(organizationId);

        organizationsApi.createRole( r1 );
        organizationsApi.createRole( r2 );

        rolePrincipal1 = new Principal( PrincipalType.ROLE, r1.getId().toString() );
        rolePrincipal2 = new Principal( PrincipalType.ROLE, r2.getId().toString() );

        // add roles to user1
        organizationsApi.addRoleToUser( organizationId, r1.getId(), user1.getId() );
        organizationsApi.addRoleToUser( organizationId, r2.getId(), user1.getId() );
    }

    @Test
    public void testAddPermission() {
        // sanity check: user1 has no permission of the entity set
        loginAs( "user1" );
        checkUserPermissions( entitySetAclKey, EnumSet.noneOf( Permission.class ) );

        // add Permissions
        loginAs( "admin" );
        EnumSet<Permission> newPermissions = EnumSet.of( Permission.DISCOVER, Permission.READ );
        Acl acl = new Acl( entitySetAclKey, ImmutableSet.of( new Ace( user1, newPermissions ) ) );

        permissionsApi.updateAcl( new AclData( acl, Action.ADD ) );

        // Check: user1 now has correct permissions of the entity set
        loginAs( "user1" );
        checkUserPermissions( entitySetAclKey, newPermissions );
    }

    @Test
    public void testSetPermission() {
        // Setup: add Permissions
        loginAs( "admin" );
        EnumSet<Permission> oldPermissions = EnumSet.of( Permission.DISCOVER, Permission.READ );
        Acl oldAcl = new Acl( entitySetAclKey, ImmutableSet.of( new Ace( user2, oldPermissions ) ) );
        permissionsApi.updateAcl( new AclData( oldAcl, Action.ADD ) );
        // sanity check: user2 has oldPermissions on entity set
        loginAs( "user2" );
        checkUserPermissions( entitySetAclKey, oldPermissions );

        // set Permissions
        loginAs( "admin" );
        EnumSet<Permission> newPermissions = EnumSet.of( Permission.DISCOVER, Permission.WRITE );
        Acl newAcl = new Acl( entitySetAclKey, ImmutableSet.of( new Ace( user2, newPermissions ) ) );
        permissionsApi.updateAcl( new AclData( newAcl, Action.SET ) );

        // Check: user2 now has new permissions of the entity set
        loginAs( "user2" );
        checkUserPermissions( entitySetAclKey, newPermissions );
    }

    @Test
    public void testRemovePermission() {
        // Setup: add Permissions
        loginAs( "admin" );
        EnumSet<Permission> oldPermissions = EnumSet.of( Permission.DISCOVER, Permission.READ );
        Acl oldAcl = new Acl( entitySetAclKey, ImmutableSet.of( new Ace( user3, oldPermissions ) ) );
        permissionsApi.updateAcl( new AclData( oldAcl, Action.ADD ) );
        // sanity check: user3 has oldPermissions on entity set
        loginAs( "user3" );
        checkUserPermissions( entitySetAclKey, oldPermissions );

        // remove Permissions
        loginAs( "admin" );
        EnumSet<Permission> remove = EnumSet.of( Permission.READ );
        EnumSet<Permission> newPermissions = EnumSet.of( Permission.DISCOVER );
        Acl newAcl = new Acl( entitySetAclKey, ImmutableSet.of( new Ace( user3, remove ) ) );
        permissionsApi.updateAcl( new AclData( newAcl, Action.REMOVE ) );

        // Check: user3 now has new permissions of the entity set
        loginAs( "user3" );
        checkUserPermissions( entitySetAclKey, newPermissions );
    }

    @Test
    public void testGetAcl() {
        loginAs( "admin" );
        EntitySet es = createEntitySet();
        AclKey aclKey = new AclKey( es.getId() );

        // sanity check: only admin has permissions
        Assert.assertEquals( 1, Iterables.size( permissionsApi.getAcl( aclKey ).getAces() ) );

        // give user1 permissions;
        EnumSet<Permission> permissions = EnumSet.of( Permission.DISCOVER, Permission.READ );
        Ace ace = new Ace( user1, permissions );
        Acl acl = new Acl( aclKey, ImmutableSet.of( ace ) );
        permissionsApi.updateAcl( new AclData( acl, Action.ADD ) );

        // Check: getAcl should return user1's permissions info, i.e. contains user1's ace
        Acl result = permissionsApi.getAcl( aclKey );
        Assert.assertTrue( Iterables.contains( result.getAces(), ace ) );
    }

    @Test
    public void testGetAclExplanation() {
        loginAs( "admin" );
        EntitySet es2 = createEntitySet();
        AclKey aclKey = new AclKey( es2.getId() );

        // add Permissions to user1: DISCOVER, to rolePrincipal1: READ, to rolePrincipal2:WRITE
        EnumSet<Permission> userPermissions = EnumSet.of( Permission.DISCOVER );
        Acl acl0 = new Acl( aclKey, ImmutableSet.of( new Ace( user1, userPermissions ) ) );
        permissionsApi.updateAcl( new AclData( acl0, Action.ADD ) );

        EnumSet<Permission> role1Permissions = EnumSet.of( Permission.READ );
        Acl acl1 = new Acl( aclKey, ImmutableSet.of( new Ace( rolePrincipal1, role1Permissions ) ) );
        permissionsApi.updateAcl( new AclData( acl1, Action.ADD ) );

        EnumSet<Permission> role2Permissions = EnumSet.of( Permission.WRITE );
        Acl acl2 = new Acl( aclKey, ImmutableSet.of( new Ace( rolePrincipal2, role2Permissions ) ) );
        permissionsApi.updateAcl( new AclData( acl2, Action.ADD ) );
        // sanity check: four aces associated to the entity set: admin, user1, rolePrincipal1, rolePrincipal2
        Assert.assertEquals( 4, Iterables.size( permissionsApi.getAcl( aclKey ).getAces() ) );

        AclExplanation aclExp = permissionsApi.getAclExplanation( aclKey );
        System.err.println( "Explanation:" + aclExp );
        // check: four aces associated to the entity set: admin, user1, rolePrincipal1, rolePrincipal2
        Assert.assertEquals( 4, Iterables.size( aclExp.getAces() ) );
        for ( AceExplanation aceExp : aclExp.getAces() ) {
            switch ( aceExp.getAce().getPrincipal().getType() ) {
                case ROLE:
                    // check: for role, the only explanation is that role is given the same permission
                    Assert.assertEquals( 1, aceExp.getExplanation().size() );
                    Assert.assertTrue( aceExp.getExplanation().contains( aceExp.getAce() ) );
                    break;
                case USER:
                    // check: for user, there could be multiple explanation
                    if ( user1.equals( aceExp.getAce().getPrincipal() ) ) {
                        Assert.assertEquals( 3, aceExp.getExplanation().size() );
                        Assert.assertTrue( aceExp.getAce().getPermissions()
                                .equals( EnumSet.of( Permission.READ, Permission.WRITE, Permission.DISCOVER ) ) );
                    }
                    break;
                default:
            }
        }

    }

}
