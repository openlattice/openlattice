package com.openlattice.users

import com.auth0.client.mgmt.ManagementAPI
import com.auth0.client.mgmt.filter.UserFilter
import com.auth0.exception.Auth0Exception
import com.auth0.json.mgmt.users.User
import com.auth0.json.mgmt.users.UsersPage
import com.auth0.jwt.algorithms.Algorithm
import com.dataloom.mappers.ObjectMappers
import com.geekbeast.auth0.*
import com.openlattice.authentication.Auth0AuthenticationConfiguration
import com.openlattice.users.export.*
import org.slf4j.LoggerFactory
import java.io.BufferedReader
import java.io.InputStreamReader
import java.security.InvalidAlgorithmParameterException
import java.time.Instant
import java.util.stream.Collectors
import java.util.zip.GZIPInputStream

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

private val logger = LoggerFactory.getLogger("Auth0Utils")

/**
 * Calls an export job to download all users from auth0 to a json and parses it to a sequence of users.
 *
 * Note: This export is used, because the user API has a 1000 user limitation.
 * @see <a href="https://auth0.com/docs/users/search/v3/get-users-endpoint#limitations"> Auth0 user endpoint
 * limitations </a>
 */
fun getUsers(auth0ApiExtension: Auth0ApiExtension): List<User> {
    val exportEntity = auth0ApiExtension.userExport()
    val job = exportEntity.submitExportJob(
            UserExportJobRequest(listOf(USER_ID, EMAIL, NICKNAME, APP_METADATA, IDENTITIES))
    )

    // will fail if export job hangs too long with too many requests error (429 status code)
    // https://auth0.com/docs/policies/rate-limits
    var exportJobResult = exportEntity.getJob(job.id)
    while (exportJobResult.status == JobStatus.pending) {
        exportJobResult = exportEntity.getJob(job.id)
    }

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
        logger.error("Couldn't read list of users from download url $downloadUrl.")
        throw e
    }

}


@Throws(Auth0Exception::class)
fun getUpdatedUsersPage(
        managementApi: ManagementAPI, lastSync: Instant, currentSync: Instant, page: Int, pageSize: Int
): UsersPage {
    require(pageSize <= MAX_PAGE_SIZE)
    { "Requested page size of $pageSize exceeds max page size of $MAX_PAGE_SIZE." }
    return managementApi.users().list(
            UserFilter()
                    .withSearchEngine(SEARCH_ENGINE_VERSION)
                    .withQuery("$UPDATED_AT={$lastSync TO $currentSync]")
                    .withFields("$USER_ID,$EMAIL,$NICKNAME,$APP_METADATA,$IDENTITIES", true)
                    .withPage(page, pageSize)
    ).execute()
}

@Throws(Auth0Exception::class)
fun getUser(managementApi: ManagementAPI, principalId: String): User {
    return managementApi.users().get(
            principalId,
            UserFilter()
                    .withSearchEngine(SEARCH_ENGINE_VERSION)
                    .withFields("$USER_ID,$EMAIL,$NICKNAME,$APP_METADATA,$IDENTITIES", true)
                    .withPage(0, MAX_PAGE_SIZE)
    ).execute()
}

internal fun parseAlgorithm(aac: Auth0AuthenticationConfiguration): Algorithm {
    val algorithm: (String) -> Algorithm = when (aac.signingAlgorithm) {
        "HS256" -> Algorithm::HMAC256
        "HS384" -> Algorithm::HMAC384
        "HS512" -> Algorithm::HMAC512
        else -> throw InvalidAlgorithmParameterException("Algorithm ${aac.signingAlgorithm} not recognized.")
    }
    return algorithm(aac.secret)
}