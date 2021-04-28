package com.openlattice.assembler

import com.openlattice.authorization.AclKey
import com.openlattice.authorization.PrincipalType
import java.util.*

private const val INTERNAL_PREFIX = "ol-internal"

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresRoles private constructor() {
    companion object {
        @JvmStatic
        fun buildOrganizationUserId(organizationId: UUID): String {
            return "$INTERNAL_PREFIX|organization|$organizationId"
        }

        @JvmStatic
        fun buildExternalPrincipalId(aclKey: AclKey, principalType: PrincipalType): String {
            return "$INTERNAL_PREFIX|${principalType.toString().toLowerCase()}|${aclKey.last()}"
        }
    }
}