package com.openlattice.directory

import com.auth0.json.mgmt.users.User
import com.openlattice.authentication.Auth0Configuration
import com.openlattice.search.Auth0UserSearchFields
import org.springframework.stereotype.Service

@Service
class LocalUserDirectoryService(auth0Configuration: Auth0Configuration) : UserDirectoryService {

    val users = auth0Configuration.users.associateBy { it.id }.toMutableMap()

    override fun getAllUsers(): Map<String, User> {
        return users
    }

    override fun getUser(userId: String): User {
        return users.getValue(userId)
    }

    override fun getUsers(userIds: Set<String>): Map<String, User> {
        return userIds.associateWith { users.getValue(it) }
    }

    override fun searchAllUsers(fields: Auth0UserSearchFields): Map<String, User> {
        val email = fields.email.orElse("")
        val name = fields.name.orElse("")
        return users.values.filter { user ->
            (listOf(user.email, user.name, user.nickname, user.givenName, user.familyName, user.username)
                    + user.identities.map { it.userId }
                    + user.identities.map { it.connection })
                .any { email.contains(it) || name.contains(it) }
        }.associateBy { it.id }
    }

    override fun deleteUser(userId: String) {
        users.remove(userId)
    }
}
