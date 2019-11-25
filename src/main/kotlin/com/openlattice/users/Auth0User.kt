package com.openlattice.users

import com.auth0.json.mgmt.users.User
import java.time.OffsetDateTime

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

class Auth0User( val user: User, val loadTime : OffsetDateTime ) {

}