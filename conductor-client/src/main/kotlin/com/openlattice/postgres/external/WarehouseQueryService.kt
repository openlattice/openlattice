package com.openlattice.postgres.external

import com.openlattice.ApiHelpers
import com.openlattice.authorization.AclKey
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.authorization.DbCredentialService
import com.openlattice.organizations.OrganizationWarehouse
import com.openlattice.postgres.external.RedshiftWarehouseCommands
import com.openlattice.postgres.external.WarehouseCommands
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.external.Schemas.*
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.util.regex.*
import java.util.*
import java.sql.Connection

private val logger = LoggerFactory.getLogger(PostgresDatabaseQueryService::class.java)

/**
 * @author Andrew Carter andrew@openlattice.com
 */

class WarehouseQueryService(
    private val assemblerConfiguration: AssemblerConfiguration,
    private val extWhManager: ExternalWarehouseConnectionManager,
    private val securePrincipalsManager: SecurePrincipalsManager,
    private val dbCredentialService: DbCredentialService
) {

    companion object {
        val MEMBER_WAREHOUSE_PERMISSIONS = setOf("CREATE", "TEMPORARY", "TEMP")
        const val PUBLIC_ROLE = "public"

        val warehouseCommands = mutableMapOf<String, WarehouseCommands>("redshift" to RedshiftWarehouseCommands)
    }

    fun createAndInitializeOrganizationWarehouse(organizationWarehouse: OrganizationWarehouse) {
        logger.info("Creating organization warehouse in ${organizationWarehouse.warehouseKey} for organization with id ${organizationWarehouse.organizationWarehouseId}")
        val (hds, orgWhName) = extWhManager.createWhAndConnect(organizationWarehouse) { orgWh ->
            createOrganizationWarehouse(orgWh)
        }
        logger.info("HikariDataSource created $hds")

        hds.let { dataSource ->
            RedshiftWarehouseCommands.createSchema(dataSource, Schemas.OPENLATTICE_SCHEMA)
            createSchema(dataSource, Schemas.UPLOAD_SCHEMA)
            configureOrganizationUser(organizationWarehouse.organizationId, dataSource)
        }
    }

    private fun createOrganizationWarehouse(organizationWarehouse: OrganizationWarehouse) {
        // Warehouse credentials should be the same as organization creds
        logger.info("Creating Organization Warehouse id ${organizationWarehouse.organizationWarehouseId}")
        val (dbOrgRole, dbAdminUserPassword) = dbCredentialService.getOrCreateOrganizationAccount(AclKey(organizationWarehouse.organizationId))
        val warehouseName = quote(organizationWarehouse.name)

//        We connect to default db in order to do initial db setup
        logger.info("Getting master connection for ${organizationWarehouse.warehouseKey}")
        getMasterConnection(organizationWarehouse.warehouseKey).use { connection ->
            connection.createStatement().use { statement ->
                logger.info("Executing statement organization warehouse setup using connection $connection")
                statement.execute(createUserInWarehouseSql(dbOrgRole, dbAdminUserPassword))
//                if (!exists(orgWhName)) {
                statement.execute(createDbSql(warehouseName))
                statement.execute(grantMemberWarehousePermissions(warehouseName, dbOrgRole))
//                }
                statement.execute(revokeAllSql(warehouseName, PUBLIC_ROLE))
            }
        }
    }

    private fun getMasterConnection(warehouseId: UUID): Connection {
        return extWhManager.connectAsSuperuser(warehouseId).connection
    }

    internal fun createUserInWarehouseSql(dbUser: String, dbUserPassword: String): String {
        return  "BEGIN;\n" +
                "      CREATE USER ${ApiHelpers.dbQuote(dbUser)} WITH PASSWORD '$dbUserPassword' NOCREATEDB NOCREATEUSER;\n" +
                "COMMIT;\n"
    }

    internal fun createDbSql(warehouseName: String): String {
        return "CREATE DATABASE $warehouseName"
    }

    internal fun createSchema(dataSource: HikariDataSource, schema: Schemas) {
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE SCHEMA IF NOT EXISTS ${schema.label}")
            }
        }
    }

    private fun configureOrganizationUser(organizationId: UUID, dataSource: HikariDataSource) {
        val dbOrgUser = quote(dbCredentialService.getDbUsername(AclKey(organizationId)))
        dataSource.connection.createStatement().use { statement ->
            statement.execute(grantOrgUserPrivilegesOnSchemaSql(OPENLATTICE_SCHEMA, dbOrgUser))
            statement.execute(setDefaultPrivilegesOnSchemaSql(UPLOAD_SCHEMA, dbOrgUser))
            statement.execute(setAdminUserDefaultPrivilegesSql(UPLOAD_SCHEMA, dbOrgUser))
        }
    }

    internal fun grantOrgUserPrivilegesOnSchemaSql(schema: Schemas, orgUserId: String): String {
        return "GRANT USAGE, CREATE ON SCHEMA ${quote(schema.label)} TO $orgUserId"
    }

    private fun setDefaultPrivilegesOnSchemaSql(schema: Schemas, usersSql: String): String {
        return "GRANT ALL PRIVILEGES ON SCHEMA ${quote(schema.label)} TO $usersSql"
    }

    private fun setAdminUserDefaultPrivilegesSql(schema: Schemas, usersSql: String): String {
        return "ALTER DEFAULT PRIVILEGES IN SCHEMA ${quote(schema.label)} GRANT ALL PRIVILEGES ON TABLES TO $usersSql"
    }

    internal fun revokeAllSql(warehouseName: String, role: String): String {
        return "REVOKE ALL ON DATABASE $warehouseName FROM $role"
    }

    internal fun grantMemberWarehousePermissions(warehouseName: String, userName: String): String {
        return "GRANT ${MEMBER_WAREHOUSE_PERMISSIONS.joinToString(", ")} " +
                "ON DATABASE $warehouseName TO ${quote(userName)}"
    }
}