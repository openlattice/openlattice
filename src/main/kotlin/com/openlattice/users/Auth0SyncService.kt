package com.openlattice.users

import com.auth0.json.mgmt.users.User
import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.module.kotlin.readValue
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.IdConstants
import com.openlattice.authorization.*
import com.openlattice.authorization.mapstores.PrincipalMapstore
import com.openlattice.authorization.mapstores.ReadSecurablePrincipalAggregator
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.organizations.SortedPrincipalSet
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.USERS
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.util.*

const val DELETE_BATCH_SIZE = 1024
private val markUserSql = "UPDATE ${USERS.name} SET ${EXPIRATION.name} = ? WHERE ${USER_ID.name} = ?"
private val expiredUsersSql = "SELECT ${USER_DATA.name} from ${USERS.name} WHERE ${EXPIRATION.name} < ? "

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
    companion object {
        private val logger = LoggerFactory.getLogger(Auth0SyncService::class.java)
    }
    private val users: IMap<String, User> = hazelcastInstance.getMap(HazelcastMap.USERS.name)
    private val principals = hazelcastInstance.getMap<AclKey,SecurablePrincipal>(HazelcastMap.PRINCIPALS.name)
    private val authnPrincipalCache = hazelcastInstance.getMap<String,SecurablePrincipal>(HazelcastMap.SECURABLE_PRINCIPALS.name)
    private val authnRolesCache = hazelcastInstance.getMap<String, SortedPrincipalSet>(HazelcastMap.RESOLVED_PRINCIPAL_TREES.name)
    private val principalTrees = hazelcastInstance.getMap<AclKey,AclKeySet>(HazelcastMap.PRINCIPAL_TREES.name)
    private val mapper = ObjectMappers.newJsonMapper()

    fun syncUser(user: User) {
        //Figure out which users need to be added to which organizations.
        //Since we don't want to do O( # organizations ) for each user, we need to lookup organizations on a per user
        //basis and see if the user needs to be added.
        logger.info("Synchronizg user ${user.id}")
        ensureSecurablePrincipalExists(user)
        val principal = getPrincipal(user)

        //Update the user in the users table before attempting processing.
        users.putIfAbsent( principal.id, user)
        processGlobalEnrollments( principal, user )
        processOrganizationEnrollments(user, principal, user.email ?: "")
        logger.info("Syncing authentication cache for ${principal.id}")
        syncAuthenticationCache(principal.id)
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

    private fun syncAuthenticationCache( principalId: String ) {
        val sp = getPrincipal(principalId) ?: return
        authnPrincipalCache.set( principalId, sp )
        val securablePrincipals = getAllPrincipals(sp) ?: return

        val currentPrincipals: NavigableSet<Principal> = TreeSet()
        currentPrincipals.add(sp.principal)
        securablePrincipals.stream()
                .map(SecurablePrincipal::getPrincipal)
                .forEach { currentPrincipals.add(it) }

        authnRolesCache.set( principalId, SortedPrincipalSet(currentPrincipals))
    }

    private fun getLayer(aclKeys: Set<AclKey>): AclKeySet {
        return AclKeySet( principalTrees.getAll(aclKeys).values.flatMap { it.value } )
    }

    private fun getAllPrincipals(sp: SecurablePrincipal): Collection<SecurablePrincipal>? {
        val roles = getLayer(setOf(sp.aclKey))
        var nextLayer: Set<AclKey> = roles

        while (nextLayer.isNotEmpty()) {
            nextLayer = getLayer(nextLayer) - roles
            roles.addAll(nextLayer)
        }

        return principals.getAll(roles).values
    }


    private fun getPrincipal(principalId: String): SecurablePrincipal? {
        return principals.aggregate(
                ReadSecurablePrincipalAggregator(),
                Predicates.equal(
                        PrincipalMapstore.PRINCIPAL_INDEX, Principals.getUserPrincipal(principalId)
                ) as Predicate<AclKey, SecurablePrincipal>
        )

    }

    private fun processGlobalEnrollments(principal: Principal, user: User) {
        orgService.addMembers(
                IdConstants.GLOBAL_ORGANIZATION_ID.id,
                setOf(principal),
                mapOf(principal to getAppMetadata(user))
        )
    }

    private fun processOrganizationEnrollments(
            user: User,
            principal: Principal,
            emailDomain: String
    ) {
        val connections = getConnections(user).values

        val missingOrgsForEmailDomains = if (emailDomain.isNotBlank()) {
            orgService.getOrganizationsWithoutUserAndWithConnectionsAndDomains(
                    principal,
                    connections,
                    emailDomain
            )
        } else setOf()

        val missingOrgsForConnections = orgService.getOrganizationsWithoutUserAndWithConnection(connections, principal)


        (missingOrgsForEmailDomains + missingOrgsForConnections).forEach { orgId ->
            orgService.addMembers(orgId, setOf(principal))
        }

    }


    fun getExpiredUsers(): BasePostgresIterable<User> {
        val expirationThreshold = System.currentTimeMillis() - 6 * REFRESH_INTERVAL_MILLIS
        return BasePostgresIterable<User>(
                PreparedStatementHolderSupplier(hds, expiredUsersSql, DELETE_BATCH_SIZE) { ps ->
                    ps.setLong(1, expirationThreshold)
                }) { rs -> mapper.readValue(rs.getString(USER_DATA.name)) }
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



