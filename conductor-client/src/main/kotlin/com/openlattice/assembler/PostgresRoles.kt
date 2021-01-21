package com.openlattice.assembler

import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Permission
import com.openlattice.authorization.PrincipalType
import java.util.UUID

private const val INTERNAL_PREFIX = "ol-internal"

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresRoles private constructor() {
    companion object {

        @JvmStatic
        fun buildPermissionRoleName(tableId: UUID, columnId: UUID, permission: Permission): String {
            return "permission_${tableId}_${columnId}_${permission.name.toLowerCase()}"
//            return "$INTERNAL_PREFIX|permission|${role.id}"
        }

        @JvmStatic
        fun buildOrganizationUserId(organizationId: UUID): String {
            return "$INTERNAL_PREFIX|organization|$organizationId"
        }

        @JvmStatic
        fun buildPostgresRoleName(roleId: UUID): String {
            return "$INTERNAL_PREFIX|role|$roleId"
        }

        @JvmStatic
        fun buildExternalPrincipalId(aclKey: AclKey, principalType: PrincipalType): String {
            return "$INTERNAL_PREFIX|${principalType.toString().toLowerCase()}|${aclKey.last()}"
        }

        @JvmStatic
        fun buildOrganizationRoleName(orgId: UUID): String {
            return buildPostgresRoleName(orgId)
        }

    }
}