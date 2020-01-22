package com.openlattice.directory

import com.auth0.json.mgmt.users.User
import com.openlattice.authentication.Auth0Configuration
import com.openlattice.directory.pojo.Auth0UserBasic
import org.springframework.stereotype.Service

@Service
class LocalUserDirectoryService(auth0Configuration: Auth0Configuration) : UserDirectoryService {
    val users = auth0Configuration.users.associateBy { it.id }
    override fun getAllUsers(): Map<String, User> {
        return users
    }

    override fun getUser(userId: String): User {
        return users.getValue(userId)
    }

    override fun searchAllUsers(searchQuery: String): Map<String, Auth0UserBasic> {
        return users.values.filter { user ->
            (listOf(user.email, user.name, user.nickname, user.givenName, user.familyName, user.username) +
                    user.identities.map { it.userId } + user.identities.map { it.connection })
                    .any { searchQuery.contains(it) }
        }.map { it.id to Auth0UserBasic(it.id, it.email, it.nickname, it.appMetadata) }.toMap()
    }
}