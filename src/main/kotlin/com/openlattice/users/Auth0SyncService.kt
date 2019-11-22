package com.openlattice.users

import com.auth0.json.mgmt.users.User
import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.module.kotlin.readValue
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.openlattice.IdConstants
import com.openlattice.authorization.Principal
import com.openlattice.authorization.PrincipalType
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.authorization.SystemRole
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.USERS
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import java.util.*

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

    fun syncUser(user: User) {
        //Figure out which users need to be added to which organizations.
        //Since we don't want to do O( # organizations ) for each user, we need to lookup organizations on a per user
        //basis and see if the user needs to be added.
        ensureSecurablePrincipalExists(user)
        val principal = getPrincipal(user)
        val roles = getRoles(user)
        val sp = spm.getPrincipal(principal.id)
        processGlobalEnrollments(sp, principal, user)
        processOrganizationEnrollments(user, sp, principal, user.email ?: "")
        markUser(user)
    }

    /**
     * @param user The user for which to read the identities.
     * @return A map of providers to connections for the specified user.
     */
    private fun getConnections(user: User): Map<String, String> {
        return user.identities.associateBy({ it.provider }, { it.connection })
    }

    private fun markUser(user: User) {
        val userId = user.id
        markUser(userId)
        users.set(userId, user)
    }

    private fun markUser(userId: String) {
        hds.connection.use { connection ->
            connection.prepareStatement(markUserSql).use { ps ->
                ps.setLong(1, System.currentTimeMillis())
                ps.setString(2, userId)
                ps.executeUpdate()
            }
        }
    }

    private fun processGlobalEnrollments(sp: SecurablePrincipal, principal: Principal, user: User) {
        if (getRoles(user).contains(SystemRole.ADMIN.getName())) {
            orgService.addMembers(
                    IdConstants.GLOBAL_ORGANIZATION_ID.id,
                    setOf(principal),
                    mapOf(principal to getAppMetadata(user))
            )
        }

    }

    private fun processOrganizationEnrollments(
            user: User, sp: SecurablePrincipal, principal: Principal, emailDomain: String
    ) {
        val connections = getConnections(user).values

        val missingOrgsForEmailDomains = if (emailDomain.isNotBlank()) {
            orgService.getOrganizationsWithoutUserAndWithConnectionsAndDomains(
                    connections, emailDomain
            )
        } else setOf()

        val missingOrgsForConnections = orgService.getOrganizationsWithoutUserAndWithConnection(connections, principal)


        (missingOrgsForEmailDomains + missingOrgsForConnections).forEach { orgId ->
            orgService.addMembers(orgId, setOf(principal))
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


