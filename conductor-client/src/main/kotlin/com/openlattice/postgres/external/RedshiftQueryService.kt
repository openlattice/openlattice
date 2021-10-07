package com.openlattice.postgres.external

import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.authorization.DbCredentialService
import com.openlattice.organizations.OrganizationDatabase

//import com.openlattice.postgres.external.PostgresDatabaseQueryService

/**
 * @author Andrew Carter andrew@openlattice.com
 */

class RedshiftQueryService(
    private val assemblerConfiguration: AssemblerConfiguration,
    private val extDbManager: ExternalDatabaseConnectionManager,
    private val securePrincipalsManager: SecurePrincipalsManager,
    private val dbCredentialService: DbCredentialService
): PostgresDatabaseQueryService(
    assemblerConfiguration,
    extDbManager,
    securePrincipalsManager,
    dbCredentialService
) {

    override fun createAndInitializeOrganizationDatabase(organizationId: UUID): OrganizationDatabase {
        logger.info("Creating organization database for organization with id $organizationId")
        val (hds, dbName) = extDbManager.createDbAndConnect(organizationId, ExternalDatabaseType.ORGANIZATION) { dbName ->
            createOrganizationDatabase(organizationId, dbName)
        }

        hds.let { dataSource ->
            configureRolesInDatabase(dataSource)
            createSchema(dataSource, Schemas.OPENLATTICE_SCHEMA)
            createSchema(dataSource, Schemas.INTEGRATIONS_SCHEMA)
            createSchema(dataSource, Schemas.STAGING_SCHEMA)
            createSchema(dataSource, Schemas.TRANSPORTER_SCHEMA)
            createSchema(dataSource, Schemas.ASSEMBLED_ENTITY_SETS)
            configureOrganizationUser(organizationId, dataSource)
            addMembersToOrganization(organizationId, dataSource, securePrincipalsManager.getOrganizationMemberPrincipals(organizationId))
            configureServerUser(dataSource)
        }

        return OrganizationDatabase(getDatabaseOid(dbName), dbName)
    }

    private fun createOrganizationDatabase() {

    }

}