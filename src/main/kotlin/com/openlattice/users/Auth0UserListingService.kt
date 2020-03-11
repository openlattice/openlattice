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
import com.auth0.json.mgmt.users.User
import com.dataloom.mappers.ObjectMappers
import com.geekbeast.auth0.*
import com.openlattice.users.export.Auth0ApiExtension
import com.openlattice.users.export.JobStatus
import com.openlattice.users.export.UserExportJobRequest
import com.openlattice.users.export.UsersList
import org.slf4j.LoggerFactory
import java.time.Instant

const val SEARCH_ENGINE_VERSION = "v3"
const val MAX_PAGE_SIZE = 100
private const val DEFAULT_PAGE_SIZE = 100

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class Auth0UserListingService(
        private val managementApi: ManagementAPI, private val auth0ApiExtension: Auth0ApiExtension
) : UserListingService {

    /**
     * Calls an export job to download all users from auth0 to a json and parses it to a sequence of users.
     *
     * Note: This export is used, because the user API has a 1000 user limitation.
     * @see <a href="https://auth0.com/docs/users/search/v3/get-users-endpoint#limitations"> Auth0 user endpoint
     * limitations </a>
     */
    override fun getAllUsers(): Sequence<User> {
        val exportEntity = auth0ApiExtension.userExport()
        val job = exportEntity.submitExportJob(
                UserExportJobRequest(listOf(USER_ID, EMAIL, NICKNAME, APP_METADATA, IDENTITIES))
        )

        var exportJob = exportEntity.getJob(job.id)
        while (exportJob.status == JobStatus.pending.name) {
            exportJob = exportEntity.getJob(job.id)
        }
        check(exportJob.status == JobStatus.expired.name) { "Export job ${job.id} expired while trying to retrieve users." }

        val users = exportJob.readUsersList()

        return users.asSequence()
    }

    /**
     * Retrieves users from auth0 where the updated_at property is larger than [from] (exclusive) and smaller than
     * [to] (inclusive) as a sequence.
     */
    override fun getUpdatedUsers(from: Instant, to: Instant): Sequence<User> {
        return Auth0UserListingResult(managementApi, from, to).asSequence()
    }
}

class Auth0UserListingResult(
        private val managementApi: ManagementAPI, private val from: Instant, private val to: Instant
) : Iterable<User> {
    override fun iterator(): Iterator<User> {
        return Auth0UserListingIterator(managementApi, from, to)
    }
}

class Auth0UserListingIterator(
        private val managementApi: ManagementAPI, private val from: Instant, private val to: Instant
) : Iterator<User> {
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
            val nextPage = getUpdatedUsersPage(managementApi, from, to, page++, DEFAULT_PAGE_SIZE).items
                    ?: listOf()
            logger.info("Loaded page {} of {} auth0 users", page - 1, nextPage.size)
            nextPage
        } catch (ex: Exception) {
            logger.error("Retrofit called failed during auth0 sync task.", ex)
            listOf()
        }
    }

}