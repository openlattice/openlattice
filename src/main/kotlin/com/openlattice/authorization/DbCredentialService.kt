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
import com.openlattice.assembler.ORGANIZATION_PREFIX
import com.openlattice.directory.MaterializedViewAccount
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.ids.HazelcastLongIdService
import org.slf4j.LoggerFactory
import java.security.SecureRandom

const val USER_PREFIX = "user"

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class DbCredentialService(hazelcastInstance: HazelcastInstance, val longIdService: HazelcastLongIdService) {

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

        private val r = SecureRandom()

        private fun generateCredential(): String {
            val cred = CharArray(CREDENTIAL_LENGTH)
            for (i in 0 until CREDENTIAL_LENGTH) {
                cred[i] = srcBuf[r.nextInt(srcBuf.size)]
            }
            return String(cred)
        }

        private fun buildPostgresUsername(securablePrincipal: SecurablePrincipal): String {
            return "ol-internal|user|${securablePrincipal.id}"
        }
    }

    private val dbCreds: IMap<String, MaterializedViewAccount> = HazelcastMap.DB_CREDS.getMap(hazelcastInstance)

    fun getDbCredential(user: SecurablePrincipal): MaterializedViewAccount? = dbCreds[buildPostgresUsername(user)]

    fun getDbCredential(userId: String): MaterializedViewAccount? = dbCreds[userId]

    fun getDbUsername(user: SecurablePrincipal): String = dbCreds.getValue(buildPostgresUsername(user)).username

    fun getDbUsername(userId: String): String = dbCreds.getValue(userId).username

    fun getOrCreateUserCredentials(user: SecurablePrincipal): MaterializedViewAccount {
        return getOrCreateUserCredentials(buildPostgresUsername(user))
    }

    fun getOrCreateUserCredentials(userId: String): MaterializedViewAccount {
        return if (dbCreds.containsKey(userId)) {
            getDbCredential(userId)!!
        } else {
            logger.info("Generating credentials for user id {}", userId)
            val cred: String = generateCredential()
            val id = longIdService.getId(scope)
            val unpaddedLength = (USER_PREFIX.length + id.toString().length)
            val username = if (userId.startsWith(ORGANIZATION_PREFIX)) {
                userId
            } else {
                if (unpaddedLength < 8) {
                    "user" + ("0".repeat(8 - unpaddedLength)) + id
                } else {
                    "user$id"
                }
            }
            val account = MaterializedViewAccount(username, cred)
            logger.info("Generated credentials for user id {} with username {}", userId, username)
            return MoreObjects.firstNonNull(dbCreds.putIfAbsent(userId, account), account)
        }
    }

    fun deleteUserCredential(user: SecurablePrincipal) {
        dbCreds.delete(buildPostgresUsername(user))
    }

    fun deleteUserCredential(userId: String) {
        dbCreds.delete(userId)
    }

    fun rollUserCredential(userId: String): String {
        val cred: String = generateCredential()
        dbCreds.set(userId, MaterializedViewAccount(dbCreds.getValue(userId).username, cred))
        return cred
    }

}

