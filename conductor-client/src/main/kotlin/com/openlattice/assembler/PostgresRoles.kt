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

        private fun createExternalDatabaseRoleIfNotExistsSql(dbRole: String): String {
            return "DO\n" +
                    "\$do\$\n" +
                    "BEGIN\n" +
                    "   IF NOT EXISTS (\n" +
                    "      SELECT\n" +
                    "      FROM   pg_catalog.pg_roles\n" +
                    "      WHERE  rolname = '$dbRole') THEN\n" +
                    "\n" +
                    "      CREATE ROLE ${ApiHelpers.dbQuote(dbRole)} NOCREATEDB NOCREATEROLE INHERIT NOLOGIN;\n" +
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
