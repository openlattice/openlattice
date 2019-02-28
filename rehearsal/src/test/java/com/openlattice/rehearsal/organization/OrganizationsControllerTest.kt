package com.openlattice.rehearsal.organization

import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organization.OrganizationsApi
import com.openlattice.rehearsal.GeneralException
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import okhttp3.RequestBody
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import java.util.UUID

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
        val url1 = OrganizationsApi.BASE +
                "/$organizationID" +
                OrganizationsApi.PRINCIPALS +
                OrganizationsApi.ROLES +
                "/$roleId" +
                OrganizationsApi.MEMBERS +
                "/${user1.id}"
        makePutRequest(url1, RequestBody.create(null, ByteArray(0)))

        val usersOfRole1 = organizationsApi.getAllUsersOfRole(organizationID, roleId).toList()
        Assert.assertEquals(1, usersOfRole1.size)
        Assert.assertEquals(user1.id, usersOfRole1[0].userId)

        // try to add role to user2 with user1
        loginAs("user1")

        val url2 = OrganizationsApi.BASE +
                "/$organizationID" +
                OrganizationsApi.PRINCIPALS +
                OrganizationsApi.ROLES +
                "/$roleId" +
                OrganizationsApi.MEMBERS +
                "/${user2.id}"
        try {
            makePutRequest(url2, RequestBody.create(null, ByteArray(0)))
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: GeneralException) {
            Assert.assertTrue(e.message!!.contains("Object [$organizationID, $roleId] is not accessible"))
        }
    }

    @Test
    fun testRemoveRoleFromPrincipal() {
        loginAs("admin")

        // add and remove role1 to/from user1 with admin
        val url1 = OrganizationsApi.BASE +
                "/$organizationID" +
                OrganizationsApi.PRINCIPALS +
                OrganizationsApi.ROLES +
                "/$roleId" +
                OrganizationsApi.MEMBERS +
                "/${user1.id}"
        makePutRequest(url1, RequestBody.create(null, ByteArray(0)))
        makeDeleteRequest(url1)

        // re-add role1 to user1 with admin
        makePutRequest(url1, RequestBody.create(null, ByteArray(0)))

        // try to remove role1 from user1 with user2
        loginAs("user2")

        try {
            makeDeleteRequest(url1)
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: GeneralException) {
            Assert.assertTrue(e.message!!.contains("Object [$organizationID, $roleId] is not accessible"))
        }
    }
}