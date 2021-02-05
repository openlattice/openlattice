package com.openlattice.assembler

import com.hazelcast.map.IMap
import com.openlattice.authorization.AccessTarget
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.PrincipalType
import java.util.*
import java.util.concurrent.CompletionStage

private const val INTERNAL_PREFIX = "ol-internal"

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresRoles private constructor() {
    companion object {

        @JvmStatic
        fun getOrCreatePermissionRolesAsync(
                permissionRoleNames: IMap<AccessTarget, UUID>,
                targets: Set<AccessTarget>,
                createExternalDatabaseRole: (roleName: String) -> Unit
        ): CompletionStage<Map<AccessTarget, UUID>> {
            val allExistingRoleNames = permissionRoleNames.getAll(targets)
            targets - allExistingRoleNames.keys
            val targetsToNewIds = targets.filterNot {
                // filter out roles that already exist
                allExistingRoleNames.containsKey(it)
            }.associateWith {
                val newRole = UUID.randomUUID()
                createExternalDatabaseRole(newRole.toString())
                newRole
            }

            val roleSetCompletion = permissionRoleNames.putAllAsync(targetsToNewIds)

            val finalRoles = mutableMapOf<AccessTarget, UUID>()
            finalRoles.putAll(allExistingRoleNames)
            finalRoles.putAll(targetsToNewIds)

            return roleSetCompletion.thenApplyAsync {
                finalRoles
            }
        }

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