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

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.MetricRegistry.name
import com.codahale.metrics.Timer
import com.google.common.eventbus.EventBus
import com.openlattice.ApiHelpers
import com.openlattice.assembler.PostgresRoles.Companion.buildOrganizationRoleName
import com.openlattice.assembler.PostgresRoles.Companion.buildOrganizationUserId
import com.openlattice.assembler.PostgresRoles.Companion.buildPostgresRoleName
import com.openlattice.authorization.*
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.organization.OrganizationEntitySetFlag
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.E
import com.openlattice.postgres.PostgresTable.PRINCIPALS
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.postgres.external.ExternalDatabasePermissionsManager
import com.openlattice.postgres.external.Schemas
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.sql.Connection
import java.sql.Statement
import java.util.*
import kotlin.NoSuchElementException

private val logger = LoggerFactory.getLogger(AssemblerConnectionManager::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class AssemblerConnectionManager(
        private val assemblerConfiguration: AssemblerConfiguration,
        private val extDbConnManager: ExternalDatabaseConnectionManager,
        private val securePrincipalsManager: SecurePrincipalsManager,
        private val organizations: HazelcastOrganizationService,
        private val dbCredentialService: DbCredentialService,
        private val extDbPermsMananger: ExternalDatabasePermissionsManager,
        eventBus: EventBus,
        metricRegistry: MetricRegistry
) {

    private val atlas: HikariDataSource = extDbConnManager.connect("postgres")
    private val materializeAllTimer: Timer =
            metricRegistry.timer(name(AssemblerConnectionManager::class.java, "materializeAll"))
    private val materializeEntitySetsTimer: Timer =
            metricRegistry.timer(name(AssemblerConnectionManager::class.java, "materializeEntitySets"))

    init {
        eventBus.register(this)
    }

    companion object {
        const val PUBLIC_ROLE = "public"

        @JvmStatic
        fun entitySetNameTableName(entitySetName: String): String {
            return "${Schemas.OPENLATTICE_SCHEMA}.${quote(entitySetName)}"
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
     * Creates a private organization database that can be used for uploading data using launchpad.
     * Also sets up foreign data wrapper using assembler in assembler so that materialized views of data can be
     * provided.
     */
    fun createAndInitializeOrganizationDatabase(organizationId: UUID, dbName: String) {
        logger.info("Creating organization database for organization with id $organizationId")
        createOrganizationDatabase(organizationId, dbName)

        extDbConnManager.connect(dbName).let { dataSource ->
            extDbPermsMananger.configureRolesInDatabase(dataSource)
            createSchema(dataSource, Schemas.OPENLATTICE_SCHEMA)
            createSchema(dataSource, Schemas.INTEGRATIONS_SCHEMA)
            createSchema(dataSource, Schemas.STAGING_SCHEMA)
            createSchema(dataSource, Schemas.TRANSPORTER_SCHEMA)
            createSchema(dataSource, Schemas.ASSEMBLED_ENTITY_SETS)
            configureOrganizationUser(organizationId, dataSource)
            addMembersToOrganization(organizationId, dataSource, organizations.getMembers(organizationId))
            configureServerUser(dataSource)
        }
    }

    internal fun createSchema(dataSource: HikariDataSource, schema: Schemas) {
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE SCHEMA IF NOT EXISTS ${schema.label}")
            }
        }
    }

    private fun configureServerUser(dataSource: HikariDataSource) {
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(
                        setSearchPathSql(
                                assemblerConfiguration.server["username"].toString(),
                                false,
                                Schemas.INTEGRATIONS_SCHEMA,
                                Schemas.TRANSPORTED_VIEWS_SCHEMA,
                                Schemas.OPENLATTICE_SCHEMA,
                                Schemas.PUBLIC_SCHEMA,
                                Schemas.STAGING_SCHEMA
                        )
                )
            }
        }
    }

    private fun configureOrganizationUser(organizationId: UUID, dataSource: HikariDataSource) {
        val dbOrgUser = quote(dbCredentialService.getDbUsername(buildOrganizationUserId(organizationId)))
        dataSource.connection.createStatement().use { statement ->
            //Allow usage and create on schema openlattice to organization user
            statement.execute(grantOrgUserPrivilegesOnSchemaSql(Schemas.OPENLATTICE_SCHEMA, dbOrgUser))
            statement.execute(setDefaultPrivilegesOnSchemaSql(Schemas.STAGING_SCHEMA, dbOrgUser))
            statement.execute(setAdminUserDefaultPrivilegesSql(Schemas.STAGING_SCHEMA, dbOrgUser))
            statement.execute(setSearchPathSql(dbOrgUser, true, Schemas.OPENLATTICE_SCHEMA, Schemas.STAGING_SCHEMA))
        }
    }

    private fun addMembersToOrganization(organizationId: UUID, dataSource: HikariDataSource, members: Set<Principal>) {
        logger.info("Configuring members for organization database {}", organizationId)
        val validUserPrincipals = members
                .filter {
                    it.id != SystemRole.OPENLATTICE.principal.id && it.id != SystemRole.ADMIN.principal.id
                }
                .filter {
                    val principalExists = securePrincipalsManager.principalExists(it)
                    if (!principalExists) {
                        logger.warn("Principal {} does not exists", it)
                    }
                    return@filter principalExists
                } //There are some bad principals in the member list some how-- probably from testing.

        val securablePrincipalsToAdd = securePrincipalsManager.getSecurablePrincipals(validUserPrincipals)
        if (securablePrincipalsToAdd.isNotEmpty()) {
            val userNames = securablePrincipalsToAdd.map { dbCredentialService.getDbUsername(it) }
            configureUsersInDatabase(dataSource, organizationId, userNames)
        }
    }

    internal fun addMembersToOrganization(
            organizationId: UUID,
            authorizedPropertyTypesOfEntitySetsByPrincipal: Map<SecurablePrincipal, Map<EntitySet, Collection<PropertyType>>>
    ) {
        extDbConnManager.connectToOrg(organizationId).let { dataSource ->
            addMembersToOrganization(organizationId, dataSource, authorizedPropertyTypesOfEntitySetsByPrincipal)
        }
    }

    private fun addMembersToOrganization(
            organizationId: UUID,
            dataSource: HikariDataSource,
            authorizedPropertyTypesOfEntitySetsByPrincipal: Map<SecurablePrincipal, Map<EntitySet, Collection<PropertyType>>>
    ) {
        if (authorizedPropertyTypesOfEntitySetsByPrincipal.isNotEmpty()) {
            val authorizedPropertyTypesOfEntitySetsByPostgresUser = authorizedPropertyTypesOfEntitySetsByPrincipal
                    .mapKeys { dbCredentialService.getDbUsername(it.key) }
            val userNames = authorizedPropertyTypesOfEntitySetsByPostgresUser.keys
            configureUsersInDatabase(dataSource, organizationId, userNames)
            dataSource.connection.use { connection ->
                grantSelectForNewMembers(connection, authorizedPropertyTypesOfEntitySetsByPostgresUser)
            }
        }
    }

    internal fun removeMembersFromOrganization(
            organizationId: UUID,
            principals: Collection<SecurablePrincipal>
    ) {
        extDbConnManager.connectToOrg(organizationId).let { dataSource ->
            if (principals.isNotEmpty()) {
                val userNames = principals.map { dbCredentialService.getDbUsername(it) }
                revokeConnectAndSchemaUsage(dataSource, organizationId, userNames)
            }
        }
    }

    internal fun updateCredentialInDatabase(unquotedUserId: String, credential: String) {
        val updateSql = updateUserCredentialSql(quote(unquotedUserId), credential)

        atlas.connection.use { connection ->
            connection.createStatement().use { stmt ->
                stmt.execute(updateSql)
            }
        }
    }

    private fun createOrganizationDatabase(organizationId: UUID, dbName: String) {
        val db = quote(dbName)
        val dbRole = buildOrganizationRoleName(dbName)

        val unquotedDbAdminUser = buildOrganizationUserId(organizationId)

        val (dbOrgUser, dbAdminUserPassword) = dbCredentialService.getOrCreateUserCredentials(unquotedDbAdminUser)

        val createOrgDbRole = extDbPermsMananger.createRoleIfNotExistsSql(dbRole)
        val createOrgDbUser = extDbPermsMananger.createUserIfNotExistsSql(unquotedDbAdminUser, dbAdminUserPassword)

        val grantRole = "GRANT ${quote(dbRole)} TO ${quote(dbOrgUser)}"
        val createDb = "CREATE DATABASE $db"
        val revokeAll = "REVOKE ALL ON DATABASE $db FROM $PUBLIC_ROLE"

        //We connect to default db in order to do initial db setup
        atlas.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(createOrgDbRole)
                statement.execute(createOrgDbUser)
                statement.execute(grantRole)
                if (!exists(dbName)) {
                    statement.execute(createDb)
                    statement.execute(
                            "GRANT ${MEMBER_ORG_DATABASE_PERMISSIONS.joinToString(", ")} " +
                                    "ON DATABASE $db TO ${quote(dbOrgUser)}"
                    )
                }
                statement.execute(revokeAll)
            }
        }
    }

    internal fun dropOrganizationDatabase(organizationId: UUID) {
        val dbName = extDbConnManager.getOrganizationDatabaseName(organizationId)
        val db = quote(dbName)
        val dbRole = quote(buildOrganizationRoleName(dbName))
        val unquotedDbAdminUser = buildOrganizationUserId(organizationId)
        val dbAdminUser = dbCredentialService.getDbUsername(unquotedDbAdminUser)

        val dropDb = " DROP DATABASE $db"
        val dropDbUser = "DROP ROLE $dbAdminUser"
        //TODO: If we grant this role to other users, we need to make sure we drop it
        val dropDbRole = "DROP ROLE $dbRole"


        //We connect to default db in order to do initial db setup

        atlas.connection.use { connection ->
            connection.createStatement().use { stmt ->
                stmt.execute(dropAllConnectionsToDatabaseSql(dbName))
            }

            connection.createStatement().use { statement ->
                statement.execute(dropDb)
                statement.execute(dropDbUser)
                statement.execute(dropDbRole)
            }
        }
    }

    internal fun materializeEntitySets(
            organizationId: UUID,
            authorizedPropertyTypesByEntitySet: Map<EntitySet, Map<UUID, PropertyType>>,
            authorizedPropertyTypesOfPrincipalsByEntitySetId: Map<UUID, Map<Principal, Set<PropertyType>>>
    ): Map<UUID, Set<OrganizationEntitySetFlag>> {
        logger.info(
                "Materializing entity sets ${authorizedPropertyTypesByEntitySet.keys.map { it.id }} in " +
                        "organization $organizationId database."
        )

        materializeAllTimer.time().use {
            extDbConnManager.connectToOrg(organizationId).let { datasource ->
                materializeEntitySets(
                        datasource,
                        authorizedPropertyTypesByEntitySet,
                        authorizedPropertyTypesOfPrincipalsByEntitySetId
                )
            }
            return authorizedPropertyTypesByEntitySet
                    .map { it.key.id to EnumSet.of(OrganizationEntitySetFlag.MATERIALIZED) }
                    .toMap()
        }
    }


    private fun materializeEntitySets(
            dataSource: HikariDataSource,
            materializablePropertyTypesByEntitySet: Map<EntitySet, Map<UUID, PropertyType>>,
            authorizedPropertyTypesOfPrincipalsByEntitySetId: Map<UUID, Map<Principal, Set<PropertyType>>>
    ) {
        materializablePropertyTypesByEntitySet.forEach { (entitySet, _) ->
            materialize(
                    dataSource,
                    entitySet,
                    authorizedPropertyTypesOfPrincipalsByEntitySetId.getValue(entitySet.id)
            )
        }
    }

    /**
     * Materializes an entity set on atlas.
     */
    private fun materialize(
            dataSource: HikariDataSource,
            entitySet: EntitySet,
            authorizedPropertyTypesOfPrincipals: Map<Principal, Set<PropertyType>>
    ) {
        materializeEntitySetsTimer.time().use {
            val tableName = entitySetNameTableName(entitySet.name)

            dataSource.connection.use { connection ->
                // first drop and create materialized view
                logger.info("Materialized entity set ${entitySet.id}")

                //Next we need to grant select on materialize view to everyone who has permission.
                val selectGrantedResults = grantSelectForEntitySet(
                        connection,
                        tableName,
                        entitySet.id,
                        authorizedPropertyTypesOfPrincipals
                )
                logger.info(
                        "Granted select for ${selectGrantedResults.filter { it >= 0 }.size} users/roles " +
                                "on materialized view $tableName"
                )
            }
        }
    }

    private fun getSelectColumnsForMaterializedView(propertyTypes: Collection<PropertyType>): List<String> {
        return listOf(ENTITY_SET_ID.name, ID_VALUE.name, ENTITY_KEY_IDS_COL.name) + propertyTypes.map {
            quote(it.type.fullQualifiedNameAsString)
        }
    }

    private fun grantSelectForEntitySet(
            connection: Connection,
            tableName: String,
            entitySetId: UUID,
            authorizedPropertyTypesOfPrincipals: Map<Principal, Set<PropertyType>>
    ): IntArray {
        // prepare batch queries
        return connection.createStatement().use { stmt ->
            authorizedPropertyTypesOfPrincipals.forEach { (principal, propertyTypes) ->
                val columns = getSelectColumnsForMaterializedView(propertyTypes)
                try {
                    val grantSelectSql = grantSelectSql(tableName, principal, columns)
                    stmt.addBatch(grantSelectSql)
                } catch (e: NoSuchElementException) {
                    logger.error("Principal $principal does not exists but has permission on entity set $entitySetId")
                }
            }
            stmt.executeBatch()
        }
    }

    private fun grantSelectForNewMembers(
            connection: Connection,
            authorizedPropertyTypesOfEntitySetsByPostgresUser: Map<String, Map<EntitySet, Collection<PropertyType>>>
    ): IntArray {
        // prepare batch queries
        return connection.createStatement().use { stmt ->
            authorizedPropertyTypesOfEntitySetsByPostgresUser
                    .forEach { (postgresUserName, authorizedPropertyTypesOfEntitySets) ->

                        // grant select on authorized tables and their properties
                        authorizedPropertyTypesOfEntitySets.forEach { (entitySet, propertyTypes) ->
                            val tableName = entitySetNameTableName(entitySet.name)
                            val columns = getSelectColumnsForMaterializedView(propertyTypes)
                            val grantSelectSql = grantSelectSql(tableName, postgresUserName, columns)
                            stmt.addBatch(grantSelectSql)
                        }

                        // also grant select on edges (if at least 1 entity set is materialized to make sure edges
                        // materialized view exist)
                        if (authorizedPropertyTypesOfEntitySets.isNotEmpty()) {
                            val edgesTableName = "${Schemas.OPENLATTICE_SCHEMA}.${E.name}"
                            val grantSelectSql = grantSelectSql(edgesTableName, postgresUserName, listOf())
                            stmt.addBatch(grantSelectSql)
                        }
                    }
            stmt.executeBatch()
        }
    }

    /**
     * Build grant select sql statement for a given table and principal with column level security.
     * If properties (columns) are left empty, it will grant select on whole table.
     */
    @Throws(NoSuchElementException::class)
    private fun grantSelectSql(
            entitySetTableName: String,
            principal: Principal,
            columns: List<String>
    ): String {
        val postgresUserName = when (principal.type) {
            PrincipalType.USER -> dbCredentialService.getDbUsername(securePrincipalsManager.getPrincipal(principal.id))
            PrincipalType.ROLE -> buildPostgresRoleName(securePrincipalsManager.lookupRole(principal))
            else -> throw IllegalArgumentException(
                    "Only ${PrincipalType.USER} and ${PrincipalType.ROLE} principal " +
                            "types can be granted select."
            )
        }

        return grantSelectSql(entitySetTableName, quote(postgresUserName), columns)
    }

    /**
     * Synchronize data changes in entity set materialized view in organization database.
     */
    internal fun refreshEntitySet(organizationId: UUID, entitySet: EntitySet) {
        logger.info("Refreshing entity set ${entitySet.id} in organization $organizationId database")
        val tableName = entitySetNameTableName(entitySet.name)

        extDbConnManager.connectToOrg(organizationId).let { dataSource ->
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
    internal fun renameMaterializedEntitySet(organizationId: UUID, newName: String, oldName: String) {
        extDbConnManager.connectToOrg(organizationId).let { dataSource ->
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
    internal fun dematerializeEntitySets(organizationId: UUID, entitySetIds: Set<UUID>) {
        extDbConnManager.connectToOrg(organizationId).let { dataSource ->
            //TODO: Implement de-materialization code here.
        }
        logger.info("Removed materialized entity sets $entitySetIds from organization $organizationId")
    }

    internal fun exists(dbName: String): Boolean {
        atlas.connection.use { connection ->
            connection.createStatement().use { stmt ->
                stmt.executeQuery("select count(*) from pg_database where datname = '$dbName'").use { rs ->
                    rs.next()
                    return rs.getInt("count") > 0
                }
            }
        }
    }

    private fun configureUsersInDatabase(dataSource: HikariDataSource, organizationId: UUID, userIds: Collection<String>) {
        val userIdsSql = userIds.joinToString(", ")

        val dbName = extDbConnManager.getOrganizationDatabaseName(organizationId)
        logger.info("Configuring users $userIds in database $dbName")
        //First we will grant all privilege which for database is connect, temporary, and create schema
        atlas.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(
                        "GRANT ${MEMBER_ORG_DATABASE_PERMISSIONS.joinToString(", ")} " +
                                "ON DATABASE ${quote(dbName)} TO $userIdsSql"
                )
            }
        }

        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                logger.info(
                        "Granting USAGE on {} schema, and granting USAGE and CREATE on {} schema for users: {}",
                        Schemas.OPENLATTICE_SCHEMA,
                        Schemas.STAGING_SCHEMA,
                        Schemas.PUBLIC_SCHEMA,
                        userIds
                )
                statement.execute("GRANT USAGE ON SCHEMA ${Schemas.OPENLATTICE_SCHEMA} TO $userIdsSql")
                statement.execute("GRANT USAGE, CREATE ON SCHEMA ${Schemas.STAGING_SCHEMA} TO $userIdsSql")
                //Set the search path for the user
                logger.info("Setting search_path to ${Schemas.OPENLATTICE_SCHEMA},${Schemas.TRANSPORTED_VIEWS_SCHEMA} for users $userIds")
                userIds.forEach { userId ->
                    statement.addBatch(setSearchPathSql(userId, true, Schemas.OPENLATTICE_SCHEMA, Schemas.STAGING_SCHEMA, Schemas.TRANSPORTED_VIEWS_SCHEMA))
                }
                statement.executeBatch()
            }
        }
    }

    private fun revokeConnectAndSchemaUsage(dataSource: HikariDataSource, organizationId: UUID, userIds: List<String>) {
        val userIdsSql = userIds.joinToString(", ")

        val dbName = extDbConnManager.getOrganizationDatabaseName(organizationId)
        logger.info(
                "Removing users $userIds from database $dbName, schema usage and all privileges on all tables in schemas {} and {}",
                Schemas.OPENLATTICE_SCHEMA,
                Schemas.STAGING_SCHEMA
        )

        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(revokePrivilegesOnDatabaseSql(dbName, userIdsSql))

                stmt.execute(revokePrivilegesOnSchemaSql(Schemas.OPENLATTICE_SCHEMA, userIdsSql))
                stmt.execute(revokePrivilegesOnTablesInSchemaSql(Schemas.OPENLATTICE_SCHEMA, userIdsSql))

                stmt.execute(revokePrivilegesOnSchemaSql(Schemas.STAGING_SCHEMA, userIdsSql))
                stmt.execute(revokePrivilegesOnTablesInSchemaSql(Schemas.STAGING_SCHEMA, userIdsSql))
            }
        }
    }

    internal fun renameOrganizationDatabase(currentDatabaseName: String, newDatabaseName: String) {
        if (checkIfDatabaseExists(newDatabaseName)) {
            throw IllegalStateException("Cannot rename database $currentDatabaseName to $newDatabaseName because database $newDatabaseName already exists")
        }

        atlas.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(dropAllConnectionsToDatabaseSql(currentDatabaseName))
            }

            conn.prepareStatement(renameDatabaseSql).use { ps ->
                ps.setString(1, currentDatabaseName)
                ps.setString(2, newDatabaseName)
                ps.execute()
            }
        }
    }

    internal fun getDatabaseOid(dbName: String): Int {
        var oid = -1
        return try {
            atlas.connection.use { conn ->
                conn.prepareStatement(databaseOidSql).use { ps ->
                    ps.setString(1, dbName)
                    val rs = ps.executeQuery()
                    if (rs.next()) {
                        oid = rs.getInt(1)
                    }
                    oid
                }
            }
        } catch (e: Exception) {
            logger.error("Unable to look up OID for database {}: ", dbName, e)
            oid
        }
    }

    internal fun createRenameDatabaseFunctionIfNotExists() {
        atlas.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(createRenameDatabaseFunctionSql)
            }
        }
    }

    private fun checkIfDatabaseExists(dbName: String): Boolean {
        atlas.connection.use { conn ->
            conn.prepareStatement(checkIfDatabaseNameIsInUseSql).use { ps ->
                ps.setString(1, dbName)
                val rs = ps.executeQuery()

                return rs.next()
            }
        }
    }
}

val MEMBER_ORG_DATABASE_PERMISSIONS = setOf("CREATE", "CONNECT", "TEMPORARY", "TEMP")

private val PRINCIPALS_SQL = "SELECT ${ACL_KEY.name} FROM ${PRINCIPALS.name} WHERE ${PRINCIPAL_TYPE.name} = ?"

internal fun grantOrgUserPrivilegesOnSchemaSql(schema: Schemas, orgUserId: String): String {
    return "GRANT USAGE, CREATE ON SCHEMA $schema TO $orgUserId"
}

private fun setDefaultPrivilegesOnSchemaSql(schema: Schemas, usersSql: String): String {
    return "GRANT ALL PRIVILEGES ON SCHEMA $schema TO $usersSql"
}

private fun setAdminUserDefaultPrivilegesSql(schema: Schemas, usersSql: String): String {
    return "ALTER DEFAULT PRIVILEGES IN SCHEMA $schema GRANT ALL PRIVILEGES ON TABLES TO $usersSql"
}

private fun setSearchPathSql(granteeId: String, isUser: Boolean, vararg schemas: Schemas): String {
    val schemasSql = schemas.joinToString()
    val granteeType = if (isUser) "USER" else "ROLE"
    return "ALTER $granteeType $granteeId SET search_path TO $schemasSql"
}

private fun revokePrivilegesOnDatabaseSql(dbName: String, usersSql: String): String {
    return "REVOKE ${MEMBER_ORG_DATABASE_PERMISSIONS.joinToString(", ")} ON DATABASE ${quote(dbName)} FROM $usersSql"
}

private fun revokePrivilegesOnSchemaSql(schema: Schemas, usersSql: String): String {
    return "REVOKE ALL PRIVILEGES ON SCHEMA ${schema.label} FROM $usersSql"
}

private fun revokePrivilegesOnTablesInSchemaSql(schema: Schemas, usersSql: String): String {
    return "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA $schema FROM $usersSql"
}

internal fun dropOwnedIfExistsSql(dbUser: String): String {
    return "DO\n" +
            "\$do\$\n" +
            "BEGIN\n" +
            "   IF EXISTS (\n" +
            "      SELECT\n" +
            "      FROM   pg_catalog.pg_roles\n" +
            "      WHERE  rolname = '$dbUser') THEN\n" +
            "\n" +
            "      DROP OWNED BY ${ApiHelpers.dbQuote(dbUser)} ;\n" +
            "   END IF;\n" +
            "END\n" +
            "\$do\$;"
}

internal fun updateUserCredentialSql(dbUser: String, credential: String): String {
    return "ALTER USER $dbUser WITH ENCRYPTED PASSWORD '$credential'"
}

internal fun dropUserIfExistsSql(dbUser: String): String {
    return "DO\n" +
            "\$do\$\n" +
            "BEGIN\n" +
            "   IF EXISTS (\n" +
            "      SELECT\n" +
            "      FROM   pg_catalog.pg_roles\n" +
            "      WHERE  rolname = '$dbUser') THEN\n" +
            "\n" +
            "      DROP ROLE ${ApiHelpers.dbQuote(dbUser)} ;\n" +
            "   END IF;\n" +
            "END\n" +
            "\$do\$;"
}

internal fun dropAllConnectionsToDatabaseSql(dbName: String): String {
    return """
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE
          pg_stat_activity.datname = '$dbName'
          AND pid <> pg_backend_pid();
    """.trimIndent()
}

internal val checkIfDatabaseNameIsInUseSql = "SELECT 1 FROM pg_database WHERE datname = ?"

internal val renameDatabaseSql = "SELECT rename_database(?, ?)"

internal val createRenameDatabaseFunctionSql = """
    CREATE OR REPLACE FUNCTION rename_database(curr_name text, new_name text) RETURNS VOID AS $$
      BEGIN
        EXECUTE 'ALTER DATABASE ' || quote_ident(curr_name) || ' RENAME TO ' || quote_ident(new_name);
      END;
    $$ LANGUAGE plpgsql
""".trimIndent()

internal val databaseOidSql = "SELECT oid FROM pg_database WHERE datname = ?"
