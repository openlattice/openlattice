package com.openlattice.users

import com.auth0.json.mgmt.users.User
import com.openlattice.authorization.Principal
import com.openlattice.authorization.PrincipalType
import org.apache.commons.validator.routines.EmailValidator

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

/**
 * @param user The user for which to read the identities.
 * @return A map of providers to connections for the specified user.
 */
fun getConnections(user: User): Map<String, String> {
    return user.identities.associateBy({ it.provider }, { it.connection })
}

fun getAppMetadata(user: User): Map<String, Set<String>> {
    return user.appMetadata.mapValues { (_, v) ->
        when (v) {
            is String -> listOf(v)
            else -> v as Collection<String>
        }.toSet()
    }
}

fun getEmailDomain(email: String): String {
    require(isValidEmail(email)) {
        "Email $email is not valid e-mail address."
    }
    return email.substring(email.indexOf("@"))
}

fun isValidEmail(email: String): Boolean {
    val atIndex = email.indexOf("@")
    return (atIndex != -1) && (atIndex != (email.length - 1)) && EmailValidator.getInstance().isValid(email)
}
