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

import com.auth0.client.mgmt.ManagementAPI
import com.auth0.client.mgmt.filter.UserFilter
import com.auth0.json.mgmt.users.User
import com.auth0.json.mgmt.users.UsersPage
import com.geekbeast.auth0.*
import org.slf4j.LoggerFactory

const val MAX_PAGE_SIZE = 100
private const val DEFAULT_PAGE_SIZE = 100

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class Auth0UserListingService(private val managementApi: ManagementAPI) : UserListingService {
    override fun getUsers(): Sequence<User> {
        return Auth0UserListingResult(managementApi).asSequence()
    }
}

class Auth0UserListingResult(private val managementApi: ManagementAPI) : Iterable<User> {
    override fun iterator(): Iterator<User> {
        return Auth0UserListingIterator(managementApi)
    }
}

class Auth0UserListingIterator(private val managementApi: ManagementAPI) : Iterator<User> {
    companion object {
        private val logger = LoggerFactory.getLogger(Auth0UserListingIterator::class.java)
    }

    var page = 0
    private var pageOfUsers = getNextPage()
    private var pageIterator = pageOfUsers.iterator()

    override fun hasNext(): Boolean {
        if (!pageIterator.hasNext()) {
            if (pageOfUsers.size == DEFAULT_PAGE_SIZE) {
                pageOfUsers = getNextPage()
                pageIterator = pageOfUsers.iterator()
            } else {
                return false
            }
        }

        return pageIterator.hasNext()
    }

    override fun next(): User {
        return pageIterator.next()
    }

    private fun getNextPage(): List<User> {
        return try {
            val nextPage = getUsersPage(managementApi, page++, DEFAULT_PAGE_SIZE).items ?: listOf()
            logger.info("Loaded page {} of {} auth0 users", page - 1, nextPage.size)
            nextPage
        } catch (ex: Exception) {
            logger.error("Retrofit called failed during auth0 sync task.", ex)
            listOf()
        }
    }

}