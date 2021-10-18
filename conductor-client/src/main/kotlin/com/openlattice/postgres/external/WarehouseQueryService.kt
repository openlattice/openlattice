package com.openlattice.postgres.external

import com.openlattice.ApiHelpers
import com.openlattice.authorization.AclKey
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.assembler.OrganizationAssembly
import com.openlattice.authorization.DbCredentialService
import com.openlattice.organizations.Organization
import com.openlattice.organizations.OrganizationWarehouse
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.external.Schemas.*
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
    }

    fun createAndInitializeOrganizationWarehouse(organizationWarehouse: OrganizationWarehouse) {
        logger.info("Creating organization warehouse in ${organizationWarehouse.warehouseKey} for organization with id ${organizationWarehouse.organizationWarehouseId}")
        val (hds, orgWhName) = extWhManager.createWhAndConnect(organizationWarehouse) { orgWh ->
            createOrganizationDatabase(orgWh)
        }
        logger.info("HikariDataSource created $hds")


//        hds.let { dataSource ->
//            configureRolesInDatabase(dataSource)
//            createSchema(dataSource, Schemas.OPENLATTICE_SCHEMA)
//            createSchema(dataSource, Schemas.INTEGRATIONS_SCHEMA)
//            createSchema(dataSource, Schemas.STAGING_SCHEMA)
//            createSchema(dataSource, Schemas.TRANSPORTER_SCHEMA)
//            createSchema(dataSource, Schemas.ASSEMBLED_ENTITY_SETS)
//            configureOrganizationUser(organizationId, dataSource)
//            addMembersToOrganization(organizationId, dataSource, securePrincipalsManager.getOrganizationMemberPrincipals(organizationId))
//            configureServerUser(dataSource)
//        }
    }

    private fun createOrganizationDatabase(organizationWarehouse: OrganizationWarehouse) {
        // Warehouse credentials should be the same as organization creds
        logger.info("Creating Organization Warehouse id ${organizationWarehouse.organizationWarehouseId}")
        val (dbOrgRole, dbAdminUserPassword) = dbCredentialService.getOrCreateOrganizationAccount(AclKey(organizationWarehouse.organizationId))
        val createOrgWhUser = createUserInWarehouseSql(dbOrgRole, dbAdminUserPassword)


        // here is where we'll need warehouse specific syntax, likely stored in a map

        val wh = quote(organizationWarehouse.name)
        val createDb = "CREATE DATABASE $wh"
        val revokeAll = "REVOKE ALL ON DATABASE $wh FROM $PUBLIC_ROLE"

//        We connect to default db in order to do initial db setup
        logger.info("Getting master connection for ${organizationWarehouse.warehouseKey}")
        getMasterConnection(organizationWarehouse.warehouseKey).use { connection ->
            connection.createStatement().use { statement ->
                logger.info("Executing statement $createOrgWhUser with $connection")
                statement.execute(createOrgWhUser)
//                if (!exists(orgWhName)) {
                logger.info("Executing statement $createDb with $connection")
                statement.execute(createDb)
                statement.execute(
                    "GRANT ${MEMBER_WAREHOUSE_PERMISSIONS.joinToString(", ")} " +
                            "ON DATABASE $wh TO ${quote(dbOrgRole)}"
                )
//                }
                statement.execute(revokeAll)
            }
        }
    }

    private fun getMasterConnection(warehouseId: UUID): Connection {
        return extWhManager.connectAsSuperuser(warehouseId).connection
    }

//    private fun configureOrganizationUser(organizationId: UUID, dataSource: HikariDataSource) {
//        val dbOrgUser = quote(dbCredentialService.getDbUsername(AclKey(organizationId)))
//        dataSource.connection.createStatement().use { statement ->
//            //Allow usage and create on schema openlattice to organization user
//            statement.execute(grantOrgUserPrivilegesOnSchemaSql(OPENLATTICE_SCHEMA, dbOrgUser))
//            statement.execute(setDefaultPrivilegesOnSchemaSql(STAGING_SCHEMA, dbOrgUser))
//            statement.execute(setAdminUserDefaultPrivilegesSql(STAGING_SCHEMA, dbOrgUser))
//            statement.execute(setSearchPathSql(dbOrgUser, true, OPENLATTICE_SCHEMA, STAGING_SCHEMA))
//        }
//    }

    internal fun createUserInWarehouseSql(dbUser: String, dbUserPassword: String): String {
        return  "BEGIN;\n" +
                "      CREATE USER ${ApiHelpers.dbQuote(dbUser)} WITH PASSWORD '$dbUserPassword' NOCREATEDB NOCREATEUSER;\n" +
                "COMMIT;\n"
    }

}