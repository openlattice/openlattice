package com.openlattice.assembler

import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Permission
import com.openlattice.authorization.PrincipalType
import com.openlattice.postgres.TableColumn
import java.util.*

private const val INTERNAL_PREFIX = "ol-internal"

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresRoles private constructor() {
    companion object {

        @JvmStatic
        fun buildPermissionRoleName(tableId: UUID, columnId: UUID, permission: Permission): String {
            return "$INTERNAL_PREFIX|permission|${tableId}_${columnId}_${permission.name.toLowerCase()}"
        }

        @JvmStatic
        fun buildPermissionRoleName(column: TableColumn, permission: Permission): String {
            return buildPermissionRoleName(column.tableId, column.columnId, permission)
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