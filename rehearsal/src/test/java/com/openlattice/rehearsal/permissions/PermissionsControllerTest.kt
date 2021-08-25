package com.openlattice.rehearsal.permissions

import com.google.common.collect.Iterables
import com.openlattice.authorization.*
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organization.OrganizationsApi
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import okhttp3.RequestBody
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.*

private lateinit var entitySetAclKey: AclKey
private lateinit var rolePrincipal1: Principal
private lateinit var rolePrincipal2: Principal


class PermissionsControllerTest : MultipleAuthenticatedUsersBase() {

    companion object {

        @JvmStatic
        @BeforeClass
        fun init() {
            loginAs("admin")
            val es = createEntitySet()
            entitySetAclKey = AclKey(es.id)

            //create roles
            val organizationID = createOrganization().id
            val role1 = TestDataFactory.role(organizationID)
            val role2 = TestDataFactory.role(organizationID)

            organizationsApi.createRole(role1)
            organizationsApi.createRole(role2)

            rolePrincipal1 = Principal(PrincipalType.ROLE, role1.principal.id.toString())
            rolePrincipal2 = Principal(PrincipalType.ROLE, role2.principal.id.toString())


            // These calls are for substituting organizationsApi.addRoleToUser, because Retrofit cannot handle URI regex
            // add role1 to user1
            val url1 = OrganizationsApi.BASE +
                    "/$organizationID" +
                    OrganizationsApi.PRINCIPALS +
                    OrganizationsApi.ROLES +
                    "/${role1.id}" +
                    OrganizationsApi.MEMBERS +
                    "/${user1.id}"
            makePutRequest(url1, RequestBody.create(null, ByteArray(0)))


            // add role2 to user1
            val url2 = OrganizationsApi.BASE +
                    "/$organizationID" +
                    OrganizationsApi.PRINCIPALS +
                    OrganizationsApi.ROLES +
                    "/${role2.id}" +
                    OrganizationsApi.MEMBERS +
                    "/${user1.id}"
            makePutRequest(url2, RequestBody.create(null, ByteArray(0)))
        }
    }

    @Test
    fun testAddPermission() {
        loginAs("user1")
        checkUserPermissions(entitySetAclKey, EnumSet.noneOf(Permission::class.java))

        //add Permissions
        loginAs("admin")
        val newPermissions = EnumSet.of(Permission.DISCOVER, Permission.READ)
        val acl = Acl(entitySetAclKey, setOf(Ace(user1, newPermissions, OffsetDateTime.MAX)))

        permissionsApi.updateAcl(AclData(acl, Action.ADD))

        //check user1 now has correct permissions of the entity set
        loginAs("user1")
        checkUserPermissions(entitySetAclKey, newPermissions)
    }

    @Test
    fun testSetPermission() {
        loginAs("admin")
        val oldPermissions = EnumSet.of(Permission.DISCOVER, Permission.READ)
        val oldAcl = Acl(entitySetAclKey, setOf(Ace(user2, oldPermissions, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(oldAcl, Action.ADD))

        loginAs("user2")
        checkUserPermissions(entitySetAclKey, oldPermissions)

        //set Permissions
        loginAs("admin")
        val newPermissions = EnumSet.of(Permission.DISCOVER, Permission.WRITE)
        val newAcl = Acl(entitySetAclKey, setOf(Ace(user2, newPermissions, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(newAcl, Action.SET))

        //check user2 now has new permissions of the entity set
        loginAs("user2")
        checkUserPermissions(entitySetAclKey, newPermissions)
    }

    @Test
    fun testRemovePermission() {
        loginAs("admin")
        val oldPermissions = EnumSet.of(Permission.DISCOVER, Permission.READ)
        val oldAcl = Acl(entitySetAclKey, setOf(Ace(user3, oldPermissions, OffsetDateTime.now(ZoneOffset.UTC))))
        permissionsApi.updateAcl(AclData(oldAcl, Action.ADD))

        loginAs("user3")
        checkUserPermissions(entitySetAclKey, oldPermissions)

        loginAs("admin")
        val remove = EnumSet.of(Permission.READ)
        val newPermissions = EnumSet.of(Permission.DISCOVER)
        val newAcl = Acl(entitySetAclKey, setOf(Ace(user3, remove, OffsetDateTime.now(ZoneOffset.UTC))))
        permissionsApi.updateAcl(AclData(newAcl, Action.REMOVE))

        loginAs("user3")
        checkUserPermissions(entitySetAclKey, newPermissions)

    }

    @Test
    fun testGetAclExplanation() {
        loginAs("admin")
        val es2 = createEntitySet()
        val aclKey = AclKey(es2.id)

        //add Permissions
        val user1Acl = Acl(aclKey, setOf(Ace(user1, EnumSet.of(Permission.DISCOVER), OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(user1Acl, Action.ADD))

        val role1Acl = Acl(aclKey, setOf(Ace(rolePrincipal1, EnumSet.of(Permission.READ), OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(role1Acl, Action.ADD))

        val role2Acl = Acl(aclKey, setOf(Ace(rolePrincipal2, EnumSet.of(Permission.WRITE), OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(role2Acl, Action.ADD))

        Assert.assertEquals(4, Iterables.size(permissionsApi.getAcl(aclKey).aces))

        val aclExplanation = permissionsApi.getAclExplanation(aclKey)
        for (aclExp in aclExplanation) {
            when (aclExp.principal.type) {
                PrincipalType.ROLE -> {
                    Assert.assertEquals(aclExp.principal, aclExp.paths[0][0])
                    Assert.assertEquals(1, aclExp.paths.size)
                }
                else -> {
                    Assert.assertEquals(2, aclExp.paths.size)
                    Assert.assertEquals(2, aclExp.paths[0].size)
                }
            }
        }
    }
}