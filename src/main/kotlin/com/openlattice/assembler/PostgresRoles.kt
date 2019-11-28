package com.openlattice.assembler

import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.organization.roles.Role
import java.util.*
import java.util.regex.Pattern

private const val ADFS_PREFIX = "adfs|"
private const val AD_PREFIX = "ad|"
private const val WAAD_PREFIX = "waad|"
private const val INTERNAL_PREFIX = "ol-internal"
private val USERNAME_REGEX = Pattern.compile(".*(\\|).*(\\|).*")
/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresRoles private constructor() {
    companion object {
        @JvmStatic
        fun buildPostgresUsername(securablePrincipal: SecurablePrincipal): String {
            return "$INTERNAL_PREFIX|user|${securablePrincipal.id}"
        }

        @JvmStatic
        fun buildOrganizationUserId(organizationId: UUID): String {
            return "$INTERNAL_PREFIX|organization|$organizationId"
        }

        @JvmStatic
        fun buildPostgresRoleName(role: Role): String {
            return "$INTERNAL_PREFIX|role|${role.id}"
        }

        @JvmStatic
        fun buildOrganizationRoleName(organizationDbName: String): String {
            return "${organizationDbName}_role"
        }

        @JvmStatic
        fun getSecurablePrincipalIdFromUserName(userName: String): UUID {
            return UUID.fromString(userName.split("|").last())
        }

        @JvmStatic
        fun isPostgresUserName(maybeUserName: String): Boolean {
            return USERNAME_REGEX.matcher(maybeUserName).matches()
        }

    }
}