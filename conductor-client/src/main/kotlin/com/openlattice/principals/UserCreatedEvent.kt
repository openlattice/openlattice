package com.openlattice.principals

import com.openlattice.authorization.SecurablePrincipal

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class UserCreatedEvent(
        val user: SecurablePrincipal
)
