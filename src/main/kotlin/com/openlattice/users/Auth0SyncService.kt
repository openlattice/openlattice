package com.openlattice.users

import com.auth0.json.mgmt.users.User
import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.module.kotlin.readValue
import com.google.common.collect.ImmutableSet
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.openlattice.authorization.*
import com.openlattice.authorization.initializers.AuthorizationInitializationTask
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.OrganizationConstants
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.USERS
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import java.util.*
import java.util.concurrent.Semaphore

const val DELETE_BATCH_SIZE = 1024
private val markUserSql = "UPDATE ${USERS.name} SET ${EXPIRATION.name} = ? WHERE ${USER_ID.name} = ?"
private val expiredUsersSql = "SELECT ${USER_ID.name} from ${USERS.name} WHERE ${EXPIRATION.name} < ? "

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class Auth0SyncService(
        hazelcastInstance: HazelcastInstance,
        private val hds: HikariDataSource,
        private val spm: SecurePrincipalsManager,
        private val orgService: HazelcastOrganizationService
) {
    private val users: IMap<String, User> = hazelcastInstance.getMap(HazelcastMap.USERS.name)

    private val mapper = ObjectMappers.newJsonMapper()
    private lateinit var userRoleAclKey: AclKey
    private lateinit var adminRoleAclKey: AclKey
    private lateinit var openlatticeOrganizationAclKey: AclKey
    private lateinit var globalOrganizationAclKey: AclKey

    private val initializationMutex = Semaphore(1)

    fun initialize() {
        if (initializationMutex.tryAcquire()) {
            userRoleAclKey = spm.lookup(AuthorizationInitializationTask.GLOBAL_USER_ROLE.principal)
            adminRoleAclKey = spm.lookup(AuthorizationInitializationTask.GLOBAL_ADMIN_ROLE.principal)
            openlatticeOrganizationAclKey = spm.lookup(OrganizationConstants.OPENLATTICE_ORG_PRINCIPAL)
            globalOrganizationAclKey = spm.lookup(OrganizationConstants.GLOBAL_ORG_PRINCIPAL)
        }
    }

    fun syncUser(user: User) {
        ensureSecurablePrincipalExists(user)
        val principal = getPrincipal(user)
        val roles = getRoles(user)
        processAutoEnrollments(principal, roles)
        markUser(user)
    }

//    private fun mapRoles(user: User, roles: Set<String>) {
//        roles.map { role ->
//            when (role) {
//                SystemRole.ADMIN.name -> adminRoleAclKey
//                SystemRole.USER.name -> userRoleAclKey
//                SystemRole.AUTHENTICATED_USER.name -> userRoleAclKey
//                else -> userRoleAclKey //spm.getAllRolesInOrganization()
//            }
//        }
//    }

    private fun getConnection(user: Principal): String {
        return user.id.substring(0, user.id.indexOf("|"))
    }

    private fun markUser(user: User) {
        val userId = user.id
        markUser(userId)
        users.set(userId, user)
    }

    private fun markUser(userId: String) {
        hds.connection.use { connection ->
            connection.prepareStatement(markUserSql).use { ps ->
                ps.setLong(1, System.currentTimeMillis() )
                ps.setString(2, userId)
                ps.executeUpdate()
            }
        }
    }

    private fun processAutoEnrollments(principal: Principal, roles: Set<String>) {

        //First do hard coded built-ins
        val sp = spm.getPrincipal(principal.id)
        if (roles.contains(SystemRole.AUTHENTICATED_USER.getName()) &&
                !spm.principalHasChildPrincipal(sp.aclKey, userRoleAclKey)) {
            orgService.addMembers(globalOrganizationAclKey[0], ImmutableSet.of(principal))
            orgService.addRoleToPrincipalInOrganization(userRoleAclKey[0], userRoleAclKey[1], principal)
        }

        if (roles.contains(SystemRole.ADMIN.getName()) &&
                !spm.principalHasChildPrincipal(sp.aclKey, adminRoleAclKey)) {
            orgService.addMembers(openlatticeOrganizationAclKey[0], ImmutableSet.of(principal))
            orgService.addRoleToPrincipalInOrganization(adminRoleAclKey[0], adminRoleAclKey[1], principal)
        }

        orgService
                .getAutoEnrollments(getConnection(principal))
                .forEach { org ->
                    //Only add to org if they aren't already a member.
                    if (!org.members.contains(principal)) {
                        orgService.addMembers(org.id, setOf(principal))
                    }
                }


    }

    fun getExpiredUsers(): BasePostgresIterable<User> {
        val expirationThreshold = System.currentTimeMillis() - 6 * REFRESH_INTERVAL_MILLIS
        return BasePostgresIterable(
                PreparedStatementHolderSupplier(hds, expiredUsersSql, DELETE_BATCH_SIZE) { ps ->
                    ps.setLong(1, expirationThreshold)
                }) { rs -> mapper.readValue<User>(rs.getString(USER_DATA.name)) }
    }

    private fun ensureSecurablePrincipalExists(user: User): Principal {
        val principal = getPrincipal(user)
        if (!spm.principalExists(principal)) {
            val principal = Principal(PrincipalType.USER, user.id)
            val title = if (user.nickname != null && user.nickname.isNotEmpty())
                user.nickname
            else
                user.email

            spm.createSecurablePrincipalIfNotExists(
                    principal,
                    SecurablePrincipal(Optional.empty(), principal, title, Optional.empty())
            )
        }
        return principal
    }

    fun remove(userId: String) {
        users.delete(userId)
    }


}


