package com.openlattice.users

import com.auth0.json.mgmt.users.User
import com.openlattice.authorization.Principal
import com.openlattice.authorization.PrincipalType
import com.openlattice.authorization.SystemRole
import com.openlattice.organizations.roles.SecurePrincipalsManager
import jodd.mail.Email
import jodd.mail.EmailAddress

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

/**
 * @return The roles of user
 */
fun getRoles(user: User): Set<String> {
    return ((user.appMetadata?.getOrDefault("roles", listOf<String>()) ?: listOf<String>()) as List<String>).toSet()
}

fun getPrincipal(user: User): Principal {
    return Principal(PrincipalType.USER, user.id)
}

fun getEmailDomain(user: User): String {
    return getEmailDomain(user.email)
}

fun getEmailDomain(email: String): String {
    require(isValidEmail(email)) {
        "Email $email is not valid e-mail address."
    }
    return email.substring(email.indexOf("@"))
}

fun getAppMetadata(user: User): Map<String, Set<String>> {
    return user.appMetadata.mapValues { (k, v) ->
        when (v) {
            is String -> listOf(v)
            else -> v as Collection<String>
        }.toSet()
    }
}

fun isValidEmail(email: String): Boolean {
    val atIndex = email.indexOf("@")
    return (atIndex != -1) && (atIndex != (email.length - 1))
}
