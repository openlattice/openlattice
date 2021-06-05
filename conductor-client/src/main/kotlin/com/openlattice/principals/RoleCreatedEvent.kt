package com.openlattice.principals

import com.openlattice.organization.roles.Role

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class RoleCreatedEvent(
        val role: Role
)
