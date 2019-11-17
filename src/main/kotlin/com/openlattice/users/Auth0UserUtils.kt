package com.openlattice.users

import com.auth0.json.mgmt.users.User
import com.openlattice.authorization.SystemRole
import com.openlattice.organizations.roles.SecurePrincipalsManager

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

/**
 * @return The roles of user
 */
private fun getRoles( user: User) : Set<String> {
    return (user.appMetadata.getOrDefault("roles", listOf<String>()) as List<String>).toSet()
}

