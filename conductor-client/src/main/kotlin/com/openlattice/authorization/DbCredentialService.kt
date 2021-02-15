/*
 * Copyright (C) 2020. OpenLattice, Inc.
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
 *
 */

package com.openlattice.authorization

import com.google.common.base.MoreObjects
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.hazelcast.query.Predicates
import com.openlattice.assembler.PostgresRoles.Companion.buildExternalPrincipalId
import com.openlattice.authorization.mapstores.PostgresCredentialMapstore
import com.openlattice.authorization.processors.GetDbUsernameFromDbCredsEntryProcessor
import com.openlattice.directory.MaterializedViewAccount
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.ids.HazelcastLongIdService
import org.slf4j.LoggerFactory
import java.security.SecureRandom
import java.util.concurrent.CompletionStage
import kotlin.math.max

const val USER_PREFIX = "user"

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class DbCredentialService(
        hazelcastInstance: HazelcastInstance,
        private val longIdService: HazelcastLongIdService
) {

    companion object {
        private val logger = LoggerFactory.getLogger(
                DbCredentialService::class.java
        )
        const val scope = "DB_USER_IDS"
        private const val upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        private const val digits = "0123456789"
        private const val special = ""//"!@#$%^&*()"

        private val lower = upper.toLowerCase()
        private val source = upper + lower + digits + special
        private val srcBuf = source.toCharArray()

        private const val CREDENTIAL_LENGTH = 29
        private const val MIN_USERNAME_LENGTH = 8

        private val r = SecureRandom()

        private fun generateCredential(): String {
            val cred = CharArray(CREDENTIAL_LENGTH)
            for (i in 0 until CREDENTIAL_LENGTH) {
                cred[i] = srcBuf[r.nextInt(srcBuf.size)]
            }
            return String(cred)
        }
    }

    private val dbCreds: IMap<AclKey, MaterializedViewAccount> = HazelcastMap.DB_CREDS.getMap(hazelcastInstance)

    fun getDbAccount(securablePrincipal: SecurablePrincipal): MaterializedViewAccount? = dbCreds[securablePrincipal.aclKey]

    fun getDbAccount(aclKey: AclKey): MaterializedViewAccount? = dbCreds[aclKey]

    fun getDbAccounts(aclKeys: Set<AclKey>): Map<AclKey, MaterializedViewAccount> {
        return dbCreds.getAll(aclKeys)
    }

    fun getDbUsername(user: SecurablePrincipal): String = getDbAccount(user)!!.username

    fun getDbUsernameAsync(user: SecurablePrincipal): CompletionStage<String> = dbCreds.getAsync(user.aclKey).thenApplyAsync { it.username }

    fun getDbUsername(aclKey: AclKey): String = getDbAccount(aclKey)!!.username

    fun getDbUsernamesByPrincipals(principals: Set<SecurablePrincipal>): Set<String> {
        return getDbUsernames(principals.mapTo(mutableSetOf<AclKey>()) { it.aclKey })
    }

    fun getDbUsernamesAsMap(aclKeys: Set<AclKey>): Map<AclKey, String> {
        return dbCreds.executeOnKeys(aclKeys, GetDbUsernameFromDbCredsEntryProcessor())
    }

    fun getDbUsernames(aclKeys: Set<AclKey>): Set<String> {
        return getDbUsernamesAsMap(aclKeys).values.toSet()
    }

    fun getOrCreateRoleAccount(securablePrincipal: SecurablePrincipal): MaterializedViewAccount {
        return getOrCreateDbAccount(securablePrincipal.aclKey, PrincipalType.ROLE)
    }

    fun getOrCreateOrganizationAccount(aclKey: AclKey): MaterializedViewAccount {
        return getOrCreateDbAccount(aclKey, PrincipalType.ORGANIZATION)
    }

    fun getOrCreateDbAccount(securablePrincipal: SecurablePrincipal): MaterializedViewAccount {
        return getOrCreateDbAccount(securablePrincipal.aclKey, PrincipalType.USER)
    }

    private fun getOrCreateDbAccount(principalAclKey: AclKey, principalType: PrincipalType): MaterializedViewAccount {
        getDbAccount(principalAclKey)?.let { return it }

        logger.info("Generating credentials for principal {}", principalAclKey)

        val username = when ( principalType ) {
            PrincipalType.ORGANIZATION, PrincipalType.ROLE  -> {
                buildExternalPrincipalId(principalAclKey, principalType)
            }
            else -> {
                val id = longIdService.getId(scope)
                val padding = "0".repeat(max(0, MIN_USERNAME_LENGTH - USER_PREFIX.length - id.toString().length))

                "$USER_PREFIX$padding$id"
            }
        }
        val cred = generateCredential()

        logger.info("Generated credential for principal {} with username {}", principalAclKey, username)

        val account = MaterializedViewAccount(username, cred)
        return MoreObjects.firstNonNull(dbCreds.putIfAbsent(principalAclKey, account), account)
    }

    fun deletePrincipalDbAccount(securablePrincipal: SecurablePrincipal) {
        dbCreds.delete(securablePrincipal.aclKey)
    }

    fun rollCredential(principalAclKey: AclKey): String {
        val cred: String = generateCredential()
        dbCreds.set(principalAclKey, MaterializedViewAccount(getDbUsername(principalAclKey), cred))
        return cred
    }

    fun getSecurablePrincipalAclKeysFromUsernames(usernames: Collection<String>): Map<String, AclKey> {
        return dbCreds.entrySet(Predicates.`in`(PostgresCredentialMapstore.USERNAME_INDEX, *usernames.toTypedArray())).associate {
            it.value.username to it.key
        }
    }
}
