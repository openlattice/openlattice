package com.openlattice.directory

import com.auth0.json.mgmt.users.User
import com.google.common.collect.ImmutableMap
import com.hazelcast.core.HazelcastInstance
import com.openlattice.auth0.Auth0TokenProvider
import com.openlattice.client.RetrofitFactory
import com.openlattice.datastore.services.Auth0ManagementApi
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.search.Auth0UserSearchFields
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

private val logger = LoggerFactory.getLogger(Auth0UserDirectoryService::class.java)

@Service
class Auth0UserDirectoryService(
        auth0TokenProvider: Auth0TokenProvider, hazelcastInstance: HazelcastInstance
) : UserDirectoryService {
    private val users = HazelcastMap.USERS.getMap(hazelcastInstance)

    private var auth0ManagementApi = RetrofitFactory
            .newClient(auth0TokenProvider.managementApiUrl) { auth0TokenProvider.token }
            .create(Auth0ManagementApi::class.java)


    override fun getAllUsers(): Map<String, User> {
        return ImmutableMap.copyOf(users)
    }

    override fun getUser(userId: String): User {
        return users.getValue(userId)
    }

    override fun getUsers(userIds: Set<String>): Map<String, User> {
        return users.getAll(userIds)
    }

    override fun deleteUser(userId: String) {
        auth0ManagementApi.deleteUser(userId)
        users.delete(userId)
    }

    //TODO: Switch over to a Hazelcast map to relieve pressure from Auth0
    override fun searchAllUsers(fields: Auth0UserSearchFields): Map<String, User> {

        var searchQuery = ""

        // https://auth0.com/docs/users/user-search/user-search-query-syntax
        // TODO - support multiple fields and construct a valid Lucene query string to pass to Auth0
        // https://jira.openlattice.com/browse/LATTICE-2805
        if (fields.email != null) {
            searchQuery = "email:${fields.email}"
        }
        else if (fields.name != null) {
            searchQuery = "name:${fields.name}"
        }

        logger.info("searching auth0 users with query: $searchQuery")

        var page = 0
        val users = mutableSetOf<User>()
        var usersPage: Set<User>?
        do {
            // Auth0 limits the total number of users you can retrieve to 1000
            // https://auth0.com/docs/users/user-search/view-search-results-by-page#limitation
            // if we start regularly hitting this limit, we'll need to switch to Auth0UserListingService
            // https://auth0.com/docs/users/import-and-export-users
            usersPage = auth0ManagementApi.searchAllUsers(
                searchQuery,
                page++,
                DEFAULT_PAGE_SIZE,
                SEARCH_ENGINE_VERSION
            )
            users.addAll(usersPage ?: setOf())
        } while (usersPage?.size == DEFAULT_PAGE_SIZE)

        if (users.isEmpty()) {
            logger.info("auth0 did not return any users for this search query: $searchQuery")
            return mapOf()
        }

        return users.associateBy { it.id }
    }
}
