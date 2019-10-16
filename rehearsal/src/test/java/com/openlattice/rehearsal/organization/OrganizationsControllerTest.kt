package com.openlattice.rehearsal.organization

import com.openlattice.authorization.*
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organization.roles.Role
import com.openlattice.rehearsal.assertException
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import java.time.OffsetDateTime
import java.util.*

class OrganizationsControllerTest : MultipleAuthenticatedUsersBase() {
    companion object {
        private lateinit var organizationID: UUID
        private lateinit var roleId: UUID

        @JvmStatic
        @BeforeClass
        fun init() {
            loginAs("admin")

            //create organization, role
            organizationID = createOrganization().id
            val role = TestDataFactory.role(organizationID)
            roleId = role.id
            organizationsApi.createRole(role)
        }
    }

    @Test
    fun testAddRoleToPrincipal() {
        loginAs("admin")

        // add role1 to user1 with admin
        OrganizationControllerCallHelper.addRoleToUser(organizationID, roleId, user1.id)

        val usersOfRole1 = organizationsApi.getAllUsersOfRole(organizationID, roleId).toList()
        Assert.assertEquals(1, usersOfRole1.size)
        Assert.assertEquals(user1.id, usersOfRole1[0].userId)

        // try to add role to user2 with user1
        loginAs("user1")
        assertException(
                { OrganizationControllerCallHelper.addRoleToUser(organizationID, roleId, user2.id) },
                "Object [$organizationID, $roleId] is not accessible"
        )
    }

    @Test
    fun testRemoveRoleFromPrincipal() {
        loginAs("admin")

        // add and remove role1 to/from user1 with admin
        OrganizationControllerCallHelper.addRoleToUser(organizationID, roleId, user1.id)
        OrganizationControllerCallHelper.removeRoleFromUser(organizationID, roleId, user1.id)

        // re-add role1 to user1 with admin
        OrganizationControllerCallHelper.addRoleToUser(organizationID, roleId, user1.id)

        // try to remove role1 from user1 with user2
        loginAs("user2")
        assertException(
                { OrganizationControllerCallHelper.removeRoleFromUser(organizationID, roleId, user1.id) },
                "Object [$organizationID, $roleId] is not accessible"
        )
    }

    @Test
    fun testAddMembersToOrganiztion() {
        // test owner access check
        loginAs("user1")
        assertException(
                { OrganizationControllerCallHelper.addMemberToOrganization(organizationID, user2.id) },
                "Object [$organizationID] is not accessible"
        )

        // test normal behavior
        loginAs("admin")

        // add ownership to user1
        val ownerPermission = EnumSet.of(Permission.OWNER)
        val acl = Acl(AclKey(organizationID), setOf(Ace(user1, ownerPermission, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(acl, Action.ADD))

        loginAs("user1")
        OrganizationControllerCallHelper.addMemberToOrganization(organizationID, user2.id)

        // clean up: remove ownership from user1, remove user2 from organization
        loginAs("admin")
        permissionsApi.updateAcl(AclData(acl, Action.REMOVE))
        OrganizationControllerCallHelper.removeMemberFromOrganization(organizationID, user2.id)
    }

    @Test
    fun testRemoveMembersFromOrganization() {
        // add member with admin
        loginAs("admin")
        OrganizationControllerCallHelper.addMemberToOrganization(organizationID, user2.id)

        // test owner access check
        loginAs("user1")
        assertException(
                { OrganizationControllerCallHelper.removeMemberFromOrganization(organizationID, user2.id) },
                "Object [$organizationID] is not accessible"
        )

        // test normal behavior
        loginAs("admin")

        // add ownership to user1
        val ownerPermission = EnumSet.of(Permission.OWNER)
        val acl = Acl(AclKey(organizationID), setOf(Ace(user1, ownerPermission, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(acl, Action.ADD))

        loginAs("user1")
        OrganizationControllerCallHelper.removeMemberFromOrganization(organizationID, user2.id)

        // clean up: remove ownership from user1
        loginAs("admin")
        permissionsApi.updateAcl(AclData(acl, Action.REMOVE))

    }

    @Test
    fun testAddPreviouslyDeletedRoleToOrganization() {

        /*
         * this test is meant to simulate the following events that may take place in the organizations web app:
         *   1. a new role is added to an organization, for example "SUPER_ADMIN"
         *   2. the new "SUPER_ADMIN" role is deleted from the organization, perhaps by accident
         *   3. the same "SUPER_ADMIN" role needs to be added back to the organization
         */

        loginAs("admin")
        val orgId = createOrganization().id

        /*
         * NOTE: this mimics how the role JSON is constructed on the frontend before the API request
         */
        val roleTitle = "SUPER_ADMIN"
        val principal = Principal(PrincipalType.ROLE, "$orgId|$roleTitle")
        val role = Role(Optional.empty(), orgId, principal, roleTitle, Optional.empty())

        val originalRoles = organizationsApi.getRoles(orgId)
        Assert.assertEquals(1, originalRoles.size)

        // 1. add a new role to the organization
        val roleId = organizationsApi.createRole(role)
        Assert.assertEquals(role.id, roleId)

        // confirm the role was successfully added
        val rolesWithSuperAdmin = organizationsApi.getRoles(orgId)
        Assert.assertEquals(2, rolesWithSuperAdmin.size)
        Assert.assertNotEquals(originalRoles, rolesWithSuperAdmin)
        Assert.assertTrue(rolesWithSuperAdmin.contains(role))

        // 2. delete the role
        organizationsApi.deleteRole(orgId, role.id)

        // confirm the role was successfully deleted
        val rolesWithoutSuperAdmin = organizationsApi.getRoles(orgId)
        Assert.assertEquals(1, rolesWithoutSuperAdmin.size)
        Assert.assertEquals(originalRoles, rolesWithoutSuperAdmin)
        Assert.assertFalse(rolesWithoutSuperAdmin.contains(role))

        // 3. add the same role to the organization
        val sameRole = Role(Optional.empty(), orgId, principal, roleTitle, Optional.empty())
        val sameRoleId = organizationsApi.createRole(sameRole)
        Assert.assertEquals(sameRole.id, sameRoleId)

        // confirm the role was successfully added
        val rolesWithSuperAdminAgain = organizationsApi.getRoles(orgId)
        Assert.assertEquals(2, rolesWithSuperAdminAgain.size)
        Assert.assertNotEquals(rolesWithoutSuperAdmin, rolesWithSuperAdminAgain)
        Assert.assertTrue(rolesWithSuperAdminAgain.contains(sameRole))
    }
}