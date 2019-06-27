package com.openlattice.rehearsal.organization

import com.openlattice.authorization.*
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.rehearsal.GeneralException
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
            organizationID = organizationsApi.createOrganizationIfNotExists(TestDataFactory.organization())
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
        try {
            OrganizationControllerCallHelper.addRoleToUser(organizationID, roleId, user2.id)
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: GeneralException) {
            Assert.assertTrue(e.message!!.contains("Object [$organizationID, $roleId] is not accessible"))
        }
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

        try {
            OrganizationControllerCallHelper.removeRoleFromUser(organizationID, roleId, user1.id)
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: GeneralException) {
            Assert.assertTrue(e.message!!.contains("Object [$organizationID, $roleId] is not accessible"))
        }
    }

    @Test
    fun testAddMembersToOrganiztion() {
        // test owner access check
        loginAs("user1")

        try {
            OrganizationControllerCallHelper.addMemberToOrganization(organizationID, user2.id)
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: GeneralException) {
            Assert.assertTrue(e.message!!.contains("Object [$organizationID] is not accessible"))
        }


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

        try {
            OrganizationControllerCallHelper.removeMemberFromOrganization(organizationID, user2.id)
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: GeneralException) {
            Assert.assertTrue(e.message!!.contains("Object [$organizationID] is not accessible"))
        }


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

}