package com.openlattice.assembler

import com.hazelcast.map.IMap
import com.openlattice.ApiHelpers
import com.openlattice.authorization.AccessTarget
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.PrincipalType
import com.zaxxer.hikari.HikariDataSource
import java.util.*
import java.util.concurrent.CompletableFuture
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
                orgDataSource: HikariDataSource
        ): CompletionStage<Map<AccessTarget, UUID>> {
            val allExistingRoleNames = permissionRoleNames.getAll(targets)
            val finalRoles = mutableMapOf<AccessTarget, UUID>()
            finalRoles.putAll(allExistingRoleNames)

            val targetsToNewIds = targets.filterNot {
                // filter out roles that already exist
                allExistingRoleNames.containsKey(it)
            }.associateWith {
                val newRole = UUID.randomUUID()
                newRole
            }

            if (targetsToNewIds.isEmpty()) {
                return CompletableFuture.supplyAsync {
                    finalRoles
                }
            }

            orgDataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    targetsToNewIds.values.forEach { roleName ->
                        stmt.execute(createExternalDatabaseRoleIfNotExistsSql(roleName.toString()))
                    }
                }
            }

            val roleSetCompletion = permissionRoleNames.putAllAsync(targetsToNewIds)
            finalRoles.putAll(targetsToNewIds)

            return roleSetCompletion.thenApplyAsync {
                finalRoles
            }
        }

        private fun createExternalDatabaseRoleIfNotExistsSql(dbRole: String): String {
            return "DO\n" +
                    "\$do\$\n" +
                    "BEGIN\n" +
                    "   IF NOT EXISTS (\n" +
                    "      SELECT\n" +
                    "      FROM   pg_catalog.pg_roles\n" +
                    "      WHERE  rolname = '$dbRole') THEN\n" +
                    "\n" +
                    "      CREATE ROLE ${ApiHelpers.dbQuote(dbRole)} NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOLOGIN;\n" +
                    "   END IF;\n" +
                    "END\n" +
                    "\$do\$;"
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