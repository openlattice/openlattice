package com.openlattice.postgres.external

import com.openlattice.ApiHelpers
import com.openlattice.postgres.external.WarehouseCommands
import java.util.UUID

/**
 * @author Andrew Carter andrew@openlattice.com
 */

class RedshiftWarehouseCommands : WarehouseCommands {

    override fun createUserInWarehouseSql(dbUser: String, dbUserPassword: String): String {
        return  "BEGIN;\n" +
                "      CREATE USER ${ApiHelpers.dbQuote(dbUser)} WITH PASSWORD '$dbUserPassword' NOCREATEDB NOCREATEUSER;\n" +
                "COMMIT;\n"
    }

    override fun createDbSql(warehouseName: String): String {
        return "CREATE DATABASE $warehouseName"
    }

    override fun createSchema(dataSource: HikariDataSource, schema: Schemas) {
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE SCHEMA IF NOT EXISTS ${schema.label}")
            }
        }
    }

    override fun configureOrganizationUser(organizationId: UUID, dataSource: HikariDataSource) {
        val dbOrgUser = quote(dbCredentialService.getDbUsername(AclKey(organizationId)))
        dataSource.connection.createStatement().use { statement ->
            statement.execute(grantOrgUserPrivilegesOnSchemaSql(Schemas.OPENLATTICE_SCHEMA, dbOrgUser))
            statement.execute(setDefaultPrivilegesOnSchemaSql(Schemas.UPLOAD_SCHEMA, dbOrgUser))
            statement.execute(setAdminUserDefaultPrivilegesSql(Schemas.UPLOAD_SCHEMA, dbOrgUser))
        }
    }

    override fun grantOrgUserPrivilegesOnSchemaSql(schema: Schemas, orgUserId: String): String {
        return "GRANT USAGE, CREATE ON SCHEMA ${quote(schema.label)} TO $orgUserId"
    }

    override fun setDefaultPrivilegesOnSchemaSql(schema: Schemas, usersSql: String): String {
        return "GRANT ALL PRIVILEGES ON SCHEMA ${quote(schema.label)} TO $usersSql"
    }

    override fun setAdminUserDefaultPrivilegesSql(schema: Schemas, usersSql: String): String {
        return "ALTER DEFAULT PRIVILEGES IN SCHEMA ${quote(schema.label)} GRANT ALL PRIVILEGES ON TABLES TO $usersSql"
    }

    override fun revokeAllSql(warehouseName: String, role: String): String {
        return "REVOKE ALL ON DATABASE $warehouseName FROM $role"
    }

    override fun grantMemberWarehousePermissions(warehouseName: String, userName: String): String {
        return "GRANT ${WarehouseQueryService.MEMBER_WAREHOUSE_PERMISSIONS.joinToString(", ")} " +
                "ON DATABASE $warehouseName TO ${quote(userName)}"
    }

}