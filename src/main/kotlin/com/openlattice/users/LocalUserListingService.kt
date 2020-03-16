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

package com.openlattice.users

import com.auth0.json.mgmt.users.User
import com.auth0.jwt.JWT
import com.geekbeast.auth0.EMAIL
import com.geekbeast.auth0.EMAIL_VERIFIED
import com.geekbeast.auth0.USER_ID
import com.openlattice.authentication.Auth0Configuration
import org.slf4j.LoggerFactory
import java.time.Instant


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class LocalUserListingService(auth0Configuration: Auth0Configuration) : UserListingService {
    companion object {
        private val logger = LoggerFactory.getLogger(LocalUserListingService::class.java)
    }

    private val users = auth0Configuration.users.associateBy { it.id }

    init {
        logger.info("************************* BEGIN JWT TOKENS *************************")
        auth0Configuration.clients.forEach { aac ->

            auth0Configuration.users.forEach { user ->
                val jwt = JWT.create()
                        .withSubject(user.id)
                        .withClaim(USER_ID, user.id)
                        .withClaim(EMAIL, user.email)
                        .withClaim(EMAIL_VERIFIED, user.isEmailVerified)
                        .withIssuer(aac.issuer)
                        .withAudience(aac.audience)
                        //TODO: Hardcoded for now, but should actually parse algorithm string
                        .sign(parseAlgorithm(aac))


                logger.info("${user.id} -> $jwt")
            }

        }
        logger.info("************************* DONE LISTING JWT *************************")

    }

    override fun getAllUsers(): Sequence<User> {
        return users.values.asSequence()
    }

    override fun getUpdatedUsers(from: Instant, to: Instant): Sequence<User> {
        return emptySequence()
    }

}

