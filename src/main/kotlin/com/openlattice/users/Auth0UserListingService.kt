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
import com.openlattice.users.export.Auth0ApiExtension
import com.openlattice.users.export.JobStatus
import com.openlattice.users.export.UserExportJobRequest
import com.openlattice.users.export.UserExportJobResult
import org.slf4j.LoggerFactory
import java.io.BufferedReader
import java.io.InputStreamReader
import java.time.Instant
import java.util.stream.Collectors
import java.util.zip.GZIPInputStream

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

    companion object {
        private val logger = LoggerFactory.getLogger(Auth0UserListingService::class.java)
    }

    /**
     * Retrieves all users from auth0 as a result of an export job.
     */
    override fun getAllUsers(): Sequence<User> {
        return getUsers(auth0ApiExtension).asSequence()
    }

    /**
     * Calls an export job to download all users from auth0 to a json and parses it to a sequence of users.
     *
     * Note: This export is used, because the user API has a 1000 user limitation.
     * @see <a href="https://auth0.com/docs/users/search/v3/get-users-endpoint#limitations"> Auth0 user endpoint
     * limitations </a>
     */
    private fun getUsers(auth0ApiExtension: Auth0ApiExtension): List<User> {
        val exportEntity = auth0ApiExtension.userExport()
        val job = exportEntity.submitExportJob(UserExportJobRequest(AUTH0_USER_FIELDS))

        // will fail if export job hangs too long with too many requests error (429 status code)
        // https://auth0.com/docs/policies/rate-limits
        var exportJobResult = exportEntity.getJob(job.id)
        while (exportJobResult.status == JobStatus.PENDING || exportJobResult.status == JobStatus.PROCESSING) {
            Thread.sleep(200) // wait before calling again for job status
            exportJobResult = exportEntity.getJob(job.id)
        }

        Thread.sleep(1000) // TODO actually fix

        return readUsersFromLocation(exportJobResult)
    }

    private fun readUsersFromLocation(exportJobResult: UserExportJobResult): List<User> {
        val downloadUrl = exportJobResult.location.get()

        try {
            val mapper = ObjectMappers.getMapper(ObjectMappers.Mapper.valueOf(exportJobResult.format.name.toUpperCase()))

            val connection = downloadUrl.openConnection()
            connection.setRequestProperty("Accept-Encoding", "gzip")

            val input = GZIPInputStream(connection.getInputStream())
            val buffered = BufferedReader(InputStreamReader(input, "UTF-8"))

            // export json format has a line by line user object format
            // if at any point we have too many users, we might have to download the file
            return buffered.lines().map { line -> mapper.readValue(line, User::class.java) }.collect(Collectors.toList())
        } catch (e: Exception) {
            logger.error("Couldn't read list of users from download url $downloadUrl.",e)
            throw e
        }
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
