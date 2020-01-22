package com.openlattice.directory

import com.auth0.json.mgmt.users.User
import com.google.common.collect.ImmutableMap
import com.hazelcast.core.HazelcastInstance
import com.openlattice.auth0.Auth0TokenProvider
import com.openlattice.client.RetrofitFactory
import com.openlattice.datastore.services.Auth0ManagementApi
import com.openlattice.directory.pojo.Auth0UserBasic
import com.openlattice.hazelcast.HazelcastMap
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

    //TODO: Switch over to a Hazelcast map to relieve pressure from Auth0
    override fun searchAllUsers(searchQuery: String): Map<String, Auth0UserBasic> {
        logger.info("Searching auth0 users with query: $searchQuery")

        var page = 0
        var pageOfUsers = auth0ManagementApi.searchAllUsers(searchQuery, page++,
                                                            DEFAULT_PAGE_SIZE
        )
        val users = mutableSetOf<Auth0UserBasic>()

        while (pageOfUsers != null) {
            users.addAll(pageOfUsers)

            if (pageOfUsers.size == DEFAULT_PAGE_SIZE) {
                pageOfUsers = auth0ManagementApi.searchAllUsers(searchQuery, page++,
                                                                DEFAULT_PAGE_SIZE
                )
            } else {
                break
            }
        }

        if (users.isEmpty()) {
            logger.warn("Auth0 did not return any users for this search.")
            return mapOf()
        }

        return users.map { it.userId to it }.toMap()
    }
}