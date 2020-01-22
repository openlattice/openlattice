package com.openlattice.users

import com.auth0.client.mgmt.ManagementAPI
import com.auth0.client.mgmt.filter.UserFilter
import com.auth0.json.mgmt.users.UsersPage
import com.auth0.jwt.algorithms.Algorithm
import com.geekbeast.auth0.*
import com.openlattice.authentication.Auth0AuthenticationConfiguration
import org.apache.commons.lang3.NotImplementedException
import java.security.InvalidAlgorithmParameterException

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
internal fun getUsersPage(managementApi: ManagementAPI, page: Int, pageSize: Int): UsersPage {
    require(pageSize <= MAX_PAGE_SIZE) { "Requested page size of $pageSize exceeds max page size of $MAX_PAGE_SIZE " }
    return managementApi.users().list(
            UserFilter()
                    .withSearchEngine("v3")
                    .withFields("$USER_ID,$EMAIL,$NICKNAME,$APP_METADATA,$IDENTITIES", true)
                    .withPage(page, pageSize)
    ).execute()
}

internal fun parseAlgorithm(aac: Auth0AuthenticationConfiguration ) : Algorithm {
    val algorithm : (String) -> Algorithm  = when(aac.signingAlgorithm) {
        "HS256" -> Algorithm::HMAC256
        "HS384" -> Algorithm::HMAC384
        "HS512" -> Algorithm::HMAC512
        else -> throw InvalidAlgorithmParameterException("Algorithm ${aac.signingAlgorithm} not recognized.")
    }
    return algorithm(aac.secret)
}