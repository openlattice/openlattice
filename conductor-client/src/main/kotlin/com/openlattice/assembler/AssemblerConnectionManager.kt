/*
 * Copyright (C) 2019. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.assembler

import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.external.DatabaseQueryManager
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.postgres.external.ExternalDatabasePermissioningService
import com.openlattice.postgres.external.Schemas.OPENLATTICE_SCHEMA
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.*

private val logger = LoggerFactory.getLogger(AssemblerConnectionManager::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class AssemblerConnectionManager(
        private val extDbManager: ExternalDatabaseConnectionManager,
        private val securePrincipalsManager: SecurePrincipalsManager,
        private val dbQueryManager: DatabaseQueryManager,
        val extDbPermissioner: ExternalDatabasePermissioningService
) {

    companion object {

        @JvmStatic
        fun entitySetNameTableName(entitySetName: String): String {
            return "$OPENLATTICE_SCHEMA.${quote(entitySetName)}"
        }

        /**
         * Build grant select sql statement for a given table and user with column level security.
         * If properties (columns) are left empty, it will grant select on whole table.
         */
        @JvmStatic
        fun grantSelectSql(
                tableName: String,
                postgresUserName: String,
                columns: List<String>
        ): String {
            val onProperties = if (columns.isEmpty()) {
                ""
            } else {
                "( ${columns.joinToString(",")} )"
            }

            return "GRANT SELECT $onProperties " +
                    "ON $tableName " +
                    "TO $postgresUserName"
        }
    }

    /**
     * Synchronize data changes in entity set materialized view in organization database.
     */
    fun refreshEntitySet(organizationId: UUID, entitySet: EntitySet) {
        logger.info("Refreshing entity set ${entitySet.id} in organization $organizationId database")
        val tableName = entitySetNameTableName(entitySet.name)

        extDbManager.connectToOrg(organizationId).let { dataSource ->
            dataSource.connection.use { connection ->
                connection.createStatement().use {
                    it.execute("REFRESH MATERIALIZED VIEW $tableName")
                }
            }
        }
    }

    /**
     * Renames a materialized view in the requested organization.
     * @param organizationId The id of the organization in which the entity set is materialized and should be renamed.
     * @param newName The new name of the entity set.
     * @param oldName The old name of the entity set.
     */
    fun renameMaterializedEntitySet(organizationId: UUID, newName: String, oldName: String) {
        extDbManager.connectToOrg(organizationId).let { dataSource ->
            dataSource.connection.createStatement().use { stmt ->
                val newTableName = quote(newName)
                val oldTableName = entitySetNameTableName(oldName)

                stmt.executeUpdate("ALTER MATERIALIZED VIEW IF EXISTS $oldTableName RENAME TO $newTableName")
            }
        }
        logger.info(
                "Renamed materialized view of entity set with old name $oldName to new name $newName in " +
                        "organization $organizationId"
        )
    }

    /**
     * Removes a materialized entity set from atlas.
     */
    fun dematerializeEntitySets(organizationId: UUID, entitySetIds: Set<UUID>) {
        extDbManager.connectToOrg(organizationId).let { dataSource ->
            //TODO: Implement de-materialization code here.
        }
        logger.info("Removed materialized entity sets $entitySetIds from organization $organizationId")
    }

    fun getAllRoles(): Set<Role> {
        return securePrincipalsManager.allRoles
    }

    fun getAllUsers(): Set<SecurablePrincipal> {
        return securePrincipalsManager.allUsers
    }

    /** DEPRECATED METHODS: MOVED TO [DatabaseQueryManager] **/

    /**
     * Creates a private organization database that can be used for uploading data using launchpad.
     * Also sets up foreign data wrapper using assembler in assembler so that materialized views of data can be
     * provided.
     */
    @Deprecated(message = "Use identical function in DatabaseQueryManager")
    fun createAndInitializeOrganizationDatabase(organizationId: UUID, dbName: String) {
        dbQueryManager.createAndInitializeOrganizationDatabase(organizationId, dbName)
    }

    @Deprecated(message = "Use identical function in DatabaseQueryManager")
    fun addMembersToOrganization(
            organizationId: UUID,
            authorizedPropertyTypesOfEntitySetsByPrincipal: Map<SecurablePrincipal, Map<EntitySet, Collection<PropertyType>>>
    ) {
        dbQueryManager.addMembersToOrganization(organizationId, authorizedPropertyTypesOfEntitySetsByPrincipal)
    }

    @Deprecated(message = "Use identical function in DatabaseQueryManager")
    fun removeMembersFromOrganization(
            organizationId: UUID,
            principals: Collection<SecurablePrincipal>
    ) {
        dbQueryManager.removeMembersFromOrganization(organizationId, principals)
    }

    @Deprecated(message = "Use identical function in DatabaseQueryManager")
    fun updateCredentialInDatabase(unquotedUserId: String, credential: String) {
        dbQueryManager.updateCredentialInDatabase(unquotedUserId, credential)
    }

    @Deprecated(message = "Use identical function in DatabaseQueryManager")
    fun renameOrganizationDatabase(currentDatabaseName: String, newDatabaseName: String) {
        dbQueryManager.renameDatabase(currentDatabaseName, newDatabaseName)
    }

    @Deprecated(message = "Use identical function in DatabaseQueryManager")
    fun getDatabaseOid(dbName: String): Int {
        return dbQueryManager.getDatabaseOid(dbName)
    }

    @Deprecated(message = "Use identical function in DatabaseQueryManager")
    fun createRenameDatabaseFunctionIfNotExists() {
        dbQueryManager.createRenameDatabaseFunctionIfNotExists()
    }
}
