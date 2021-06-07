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

package com.openlattice.postgres.external

import com.openlattice.ApiHelpers
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.authorization.*
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.OrganizationDatabase
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresProjectionService.Companion.RENAME_SERVER_DB_FUNCTION
import com.openlattice.postgres.PostgresTable.E
import com.openlattice.postgres.external.Schemas.*
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.sql.Connection
import java.util.*

private val logger = LoggerFactory.getLogger(PostgresDatabaseQueryService::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class PostgresDatabaseQueryService(
        private val assemblerConfiguration: AssemblerConfiguration,
        private val extDbManager: ExternalDatabaseConnectionManager,
        private val securePrincipalsManager: SecurePrincipalsManager,
        private val dbCredentialService: DbCredentialService
) : DatabaseQueryManager {

    companion object {
        const val PUBLIC_ROLE = "public"

        /**
         * Build grant select sql statement for a given table and user with column level security.
         * If properties (columns) are left empty, it will grant select on whole table.
         */
        @JvmStatic
        private fun grantSelectSql(
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

        @JvmStatic
        fun entitySetNameTableName(entitySetName: String): String {
            return "$OPENLATTICE_SCHEMA.${quote(entitySetName)}"
        }
    }

    private fun getAtlasConnection(): Connection {
        return extDbManager.connectAsSuperuser().connection
    }

    /**
     * Creates a private organization database that can be used for uploading data using launchpad.
     * Also sets up foreign data wrapper using assembler in assembler so that materialized views of data can be
     * provided.
     */
    override fun createAndInitializeOrganizationDatabase(organizationId: UUID): OrganizationDatabase {
        logger.info("Creating organization database for organization with id $organizationId")
        val (hds, dbName) = extDbManager.createDbAndConnect(organizationId, ExternalDatabaseType.ORGANIZATION) { dbName ->
            createOrganizationDatabase(organizationId, dbName)
        }

        hds.let { dataSource ->
            configureRolesInDatabase(dataSource)
            createSchema(dataSource, OPENLATTICE_SCHEMA)
            createSchema(dataSource, INTEGRATIONS_SCHEMA)
            createSchema(dataSource, STAGING_SCHEMA)
            createSchema(dataSource, TRANSPORTER_SCHEMA)
            createSchema(dataSource, ASSEMBLED_ENTITY_SETS)
            configureOrganizationUser(organizationId, dataSource)
            addMembersToOrganization(organizationId, dataSource, securePrincipalsManager.getOrganizationMemberPrincipals(organizationId))
            configureServerUser(dataSource)
        }

        return OrganizationDatabase(getDatabaseOid(dbName), dbName)
    }

    override fun createAndInitializeCollaborationDatabase(collaborationId: UUID): OrganizationDatabase {
        logger.info("Creating collaboration database for collaboration with id $collaborationId")
        val (hds, dbName) = extDbManager.createDbAndConnect(collaborationId, ExternalDatabaseType.COLLABORATION) { dbName ->
            createDatabase(dbName)
        }

        hds.let { dbHds ->
            createRenameServerFunctionIfNotExists(dbHds)
            configureRolesInDatabase(dbHds)
        }

        return OrganizationDatabase(getDatabaseOid(dbName), dbName)
    }

    override fun addMembersToCollaboration(collaborationId: UUID, memberRoles: Collection<String>) {
        if (memberRoles.isEmpty()) {
            return
        }

        executeStatementInDatabase(collaborationId) { dbName ->
            collaborationMemberGrantSql(dbName, memberRoles)
        }
    }

    override fun collaborationMemberGrantSql(dbName: String, memberRoles: Iterable<String>): String {
        return """
            GRANT ${MEMBER_DATABASE_PERMISSIONS.joinToString()}
            ON DATABASE ${quote(dbName)}
            TO ${memberRoles.joinToString()}
        """.trimIndent()
    }

    override fun removeMembersFromCollaboration(collaborationId: UUID, memberRoles: Collection<String>) {
        if (memberRoles.isEmpty()) {
            return
        }

        executeStatementInDatabase(collaborationId) { dbName ->
            collaborationMemberRevokeSql(dbName, memberRoles)
        }
    }

    override fun collaborationMemberRevokeSql(dbName: String, memberRoles: Iterable<String>): String {
        return """
            REVOKE ALL
            ON DATABASE ${quote(dbName)}
            FROM ${memberRoles.joinToString()}
        """.trimIndent()
    }

    override fun createAndInitializeSchemas(collaborationId: UUID, schemaNameToAuthorizedPgRoles: Map<String, Collection<String>>) {
        executeStatementsInDatabase(collaborationId) {
            val sqlStatements = mutableSetOf<String>()

            schemaNameToAuthorizedPgRoles.forEach { (schemaName, authorizedRoles) ->
                sqlStatements.add(createSchemaSql(schemaName))
                if (authorizedRoles.isNotEmpty()) {
                    sqlStatements.add(grantUsageOnSchemaToRolesSql(schemaName, authorizedRoles))
                }
            }

            sqlStatements
        }
    }

    override fun addMembersToCollabInSchema(collaborationId: UUID, schemaName: String, members: Collection<String>) {
        if (members.isEmpty()) {
            return
        }

        executeStatementsInDatabase(collaborationId) { dbName ->
            setOf(
                    collaborationMemberGrantSql(dbName, members),
                    grantUsageOnSchemaToRolesSql(schemaName, members)
            )
        }
    }

    override fun removeMembersFromSchemaInCollab(collaborationId: UUID, schemaName: String, members: Collection<String>) {
        if (members.isEmpty()) {
            return
        }

        executeStatementInDatabase(collaborationId) {
            revokeUsageOnSchemaToRolesSql(schemaName, members)
        }
    }

    override fun removeMembersFromDatabaseInCollab(collaborationId: UUID, members: Collection<String>) {
        if (members.isEmpty()) {
            return
        }

        executeStatementInDatabase(collaborationId) { dbName ->
            collaborationMemberRevokeSql(dbName, members)
        }
    }

    override fun dropSchemas(collaborationId: UUID, schemasToDrop: Iterable<String>) {
        executeStatementsInDatabase(collaborationId) {
            schemasToDrop.map { dropSchemaSql(it) }
        }
    }


    internal fun createSchema(dataSource: HikariDataSource, schema: Schemas) {
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE SCHEMA IF NOT EXISTS ${schema.label}")
            }
        }
    }

    override fun renameSchema(collaborationId: UUID, oldName: String, newName: String) {
        executeStatementInDatabase(collaborationId) {
            renameSchemaSql(oldName, newName)
        }
    }

    private fun configureServerUser(dataSource: HikariDataSource) {
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(
                        setSearchPathSql(
                                assemblerConfiguration.server["username"].toString(),
                                false,
                                INTEGRATIONS_SCHEMA,
                                ENTERPRISE_FDW_SCHEMA,
                                OPENLATTICE_SCHEMA,
                                PUBLIC_SCHEMA,
                                STAGING_SCHEMA
                        )
                )
            }
        }
    }

    private fun configureOrganizationUser(organizationId: UUID, dataSource: HikariDataSource) {
        val dbOrgUser = quote(dbCredentialService.getDbUsername(AclKey(organizationId)))
        dataSource.connection.createStatement().use { statement ->
            //Allow usage and create on schema openlattice to organization user
            statement.execute(grantOrgUserPrivilegesOnSchemaSql(OPENLATTICE_SCHEMA, dbOrgUser))
            statement.execute(setDefaultPrivilegesOnSchemaSql(STAGING_SCHEMA, dbOrgUser))
            statement.execute(setAdminUserDefaultPrivilegesSql(STAGING_SCHEMA, dbOrgUser))
            statement.execute(setSearchPathSql(dbOrgUser, true, OPENLATTICE_SCHEMA, STAGING_SCHEMA))
        }
    }

    override fun addMembersToOrganization(organizationId: UUID, dataSource: HikariDataSource, members: Set<Principal>) {
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

    override fun addMembersToOrganization(
            organizationId: UUID,
            authorizedPropertyTypesOfEntitySetsByPrincipal: Map<SecurablePrincipal, Map<EntitySet, Collection<PropertyType>>>
    ) {
        extDbManager.connectToOrg(organizationId).let { dataSource ->
            addMembersToOrganization(organizationId, dataSource, authorizedPropertyTypesOfEntitySetsByPrincipal)
        }
    }

    override fun addMembersToOrganization(
            organizationId: UUID,
            dataSource: HikariDataSource,
            authorizedPropertyTypesOfEntitySetsByPrincipal: Map<SecurablePrincipal, Map<EntitySet, Collection<PropertyType>>>
    ) {
        if (authorizedPropertyTypesOfEntitySetsByPrincipal.isEmpty()) {
            return
        }

        val authorizedPropertyTypesOfEntitySetsByPostgresUser = authorizedPropertyTypesOfEntitySetsByPrincipal
                .mapKeys { dbCredentialService.getDbUsername(it.key) }
        val userNames = authorizedPropertyTypesOfEntitySetsByPostgresUser.keys
        configureUsersInDatabase(dataSource, organizationId, userNames)
        dataSource.connection.use { connection ->
            grantSelectForNewMembers(connection, authorizedPropertyTypesOfEntitySetsByPostgresUser)
        }

    }

    override fun removeMembersFromOrganization(
            organizationId: UUID,
            principals: Collection<SecurablePrincipal>
    ) {
        if (principals.isEmpty()) {
            return
        }

        val roleNames = dbCredentialService.getDbAccounts(principals.map { it.aclKey }.toSet()).map { it.value.username }

        extDbManager.connectToOrg(organizationId).let { dataSource ->
            revokeConnectAndSchemaUsage(dataSource, organizationId, roleNames)
        }
    }

    override fun updateCredentialInDatabase(unquotedUserId: String, credential: String) {
        val updateSql = updateUserCredentialSql(quote(unquotedUserId), credential)

        getAtlasConnection().use { connection ->
            connection.createStatement().use { stmt ->
                stmt.execute(updateSql)
            }
        }
    }

    private fun createOrganizationDatabase(organizationId: UUID, dbName: String) {
        val (dbOrgRole, dbAdminUserPassword) = dbCredentialService.getOrCreateOrganizationAccount(AclKey(organizationId))
        val createOrgDbUser = createUserIfNotExistsSql(dbOrgRole, dbAdminUserPassword)

        val db = quote(dbName)
        val createDb = "CREATE DATABASE $db"
        val revokeAll = "REVOKE ALL ON DATABASE $db FROM $PUBLIC_ROLE"

//        We connect to default db in order to do initial db setup
        getAtlasConnection().use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(createOrgDbUser)
                if (!exists(dbName)) {
                    statement.execute(createDb)
                    statement.execute(
                            "GRANT ${MEMBER_DATABASE_PERMISSIONS.joinToString(", ")} " +
                                    "ON DATABASE $db TO ${quote(dbOrgRole)}"
                    )
                }
                statement.execute(revokeAll)
            }
        }
    }

    override fun createDatabase(dbName: String) {
        val db = quote(dbName)

        val createDb = "CREATE DATABASE $db"
        val revokeAll = "REVOKE ALL ON DATABASE $db FROM $PUBLIC_ROLE"

        //We connect to default db in order to do initial db setup
        getAtlasConnection().use { connection ->
            connection.createStatement().use { statement ->
                if (!exists(dbName)) {
                    statement.execute(createDb)
                }
                statement.execute(revokeAll)
            }
        }
    }

    override fun dropOrganizationDatabase(organizationId: UUID) {
        val dbName = extDbManager.getDatabaseName(organizationId)
        dropDatabase(dbName)

        val dbAdminUser = quote(dbCredentialService.getDbUsername(AclKey(organizationId)))
        val dropDbUser = "DROP ROLE $dbAdminUser"

        // Drop organization-specific database roles

        getAtlasConnection().use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(dropDbUser)
            }
        }
    }


    override fun dropDatabase(dbName: String) {
        val dropDbSql = "DROP DATABASE ${quote(dbName)}"

        //We connect to default db in order to do initial db setup

        getAtlasConnection().use { connection ->
            connection.createStatement().use { stmt ->
                stmt.execute(dropAllConnectionsToDatabaseSql(dbName))
            }

            connection.createStatement().use { statement ->
                statement.execute(dropDbSql)
            }
        }
    }

    internal fun executeStatementInDatabase(organizationId: UUID, sqlFromDbName: (String) -> String) {
        executeStatementsInDatabase(organizationId) { setOf(sqlFromDbName(it)) }
    }

    internal fun executeStatementsInDatabase(organizationId: UUID, sqlFromDbName: (String) -> Iterable<String>) {
        val (hds, dbName) = extDbManager.connectToOrgGettingName(organizationId)
        val sqlStatements = sqlFromDbName(dbName)
        hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                sqlStatements.forEach { stmt.execute(it) }
            }
        }
    }

    private fun getSelectColumnsForMaterializedView(propertyTypes: Collection<PropertyType>): List<String> {
        return listOf(ENTITY_SET_ID.name, ID_VALUE.name, ENTITY_KEY_IDS_COL.name) + propertyTypes.map {
            quote(it.type.fullQualifiedNameAsString)
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
                            val edgesTableName = "$OPENLATTICE_SCHEMA.${E.name}"
                            val grantSelectSql = grantSelectSql(edgesTableName, postgresUserName, listOf())
                            stmt.addBatch(grantSelectSql)
                        }
                    }
            stmt.executeBatch()
        }
    }

    internal fun exists(dbName: String): Boolean {
        getAtlasConnection().use { connection ->
            connection.createStatement().use { stmt ->
                stmt.executeQuery("select count(*) from pg_database where datname = '$dbName'").use { rs ->
                    rs.next()
                    return rs.getInt("count") > 0
                }
            }
        }
    }

    override fun getAllRoles(): Set<Role> {
        return securePrincipalsManager.allRoles
    }

    override fun getAllUsers(): Set<SecurablePrincipal> {
        return securePrincipalsManager.allUsers
    }

    private fun configureRolesInDatabase(dataSource: HikariDataSource) {
        val roles = getAllRoles()

        if (roles.isEmpty()) {
            return
        }
        val roleSql = dbCredentialService.getDbUsernamesByPrincipals(roles).joinToString {
            quote(it)
        }

        logger.info("Revoking $PUBLIC_SCHEMA schema right from all roles")
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                //Don't allow users to access public schema which will contain foreign data wrapper tables.
                statement.execute("REVOKE USAGE ON SCHEMA $PUBLIC_SCHEMA FROM $roleSql")
            }
        }
    }

    override fun dropUserIfExists(user: SecurablePrincipal) {
        getAtlasConnection().use { connection ->
            connection.createStatement().use { statement ->
                //TODO: Go through every database and for old users clean them out.
//                    logger.info("Attempting to drop owned by old name {}", user.name)
//                    statement.execute(dropOwnedIfExistsSql(user.name))
                logger.info("Attempting to drop user {}", user.name)
                statement.execute(dropUserIfExistsSql(user.name)) //Clean out the old users.
                dbCredentialService.deletePrincipalDbAccount(user)
                //Don't allow users to access public schema which will contain foreign data wrapper tables.
                logger.info("Revoking $PUBLIC_SCHEMA schema right from user {}", user)
            }
        }
    }

    private fun configureUsersInDatabase(dataSource: HikariDataSource, organizationId: UUID, userIds: Collection<String>) {
        if (userIds.isEmpty()) {
            return
        }

        val userIdsSql = userIds.joinToString()

        val dbName = extDbManager.getDatabaseName(organizationId)
        logger.info("Configuring users $userIds in database $dbName")
        //First we will grant all privilege which for database is connect, temporary, and create schema
        getAtlasConnection().use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(
                        "GRANT ${MEMBER_DATABASE_PERMISSIONS.joinToString(", ")} " +
                                "ON DATABASE ${quote(dbName)} TO $userIdsSql"
                )
            }
        }

        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                logger.info(
                        "Granting USAGE and CREATE on {} and {} schemas for users: {}",
                        OPENLATTICE_SCHEMA,
                        STAGING_SCHEMA,
                        userIds
                )
                statement.execute("GRANT USAGE, CREATE ON SCHEMA $OPENLATTICE_SCHEMA TO $userIdsSql")
                statement.execute("GRANT USAGE, CREATE ON SCHEMA $STAGING_SCHEMA TO $userIdsSql")
                //Set the search path for the user
                logger.info("Setting search_path to $OPENLATTICE_SCHEMA,$ASSEMBLED_ENTITY_SETS for users $userIds")
                userIds.forEach { userId ->
                    statement.addBatch(setSearchPathSql(userId, true, OPENLATTICE_SCHEMA, STAGING_SCHEMA, ASSEMBLED_ENTITY_SETS))
                }
                statement.executeBatch()
            }
        }
    }

    private fun revokeConnectAndSchemaUsage(dataSource: HikariDataSource, organizationId: UUID, userIds: List<String>) {
        if (userIds.isEmpty()) {
            return
        }

        val userIdsSql = userIds.joinToString(", ")

        val dbName = extDbManager.getDatabaseName(organizationId)
        logger.info(
                "Removing users $userIds from database $dbName, schema usage and all privileges on all tables in schemas {} and {}",
                OPENLATTICE_SCHEMA,
                STAGING_SCHEMA
        )

        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(revokePrivilegesOnDatabaseSql(dbName, userIdsSql))

                stmt.execute(revokePrivilegesOnSchemaSql(OPENLATTICE_SCHEMA, userIdsSql))
                stmt.execute(revokePrivilegesOnTablesInSchemaSql(OPENLATTICE_SCHEMA, userIdsSql))

                stmt.execute(revokePrivilegesOnSchemaSql(STAGING_SCHEMA, userIdsSql))
                stmt.execute(revokePrivilegesOnTablesInSchemaSql(STAGING_SCHEMA, userIdsSql))
            }
        }
    }

    override fun renameDatabase(currentDatabaseName: String, newDatabaseName: String) {
        if (checkIfDatabaseExists(newDatabaseName)) {
            throw IllegalStateException("Cannot rename database $currentDatabaseName to $newDatabaseName because database $newDatabaseName already exists")
        }

        getAtlasConnection().use { conn ->
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

    override fun getDatabaseOid(dbName: String): Int {
        var oid = -1
        return try {
            getAtlasConnection().use { conn ->
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

    override fun createRenameDatabaseFunctionIfNotExists() {
        getAtlasConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(createRenameDatabaseFunctionSql)
            }
        }
    }

    private fun createRenameServerFunctionIfNotExists(hikariDataSource: HikariDataSource) {
        hikariDataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(createRenameServerDatabaseFunctionSql)
            }
        }
    }

    private fun checkIfDatabaseExists(dbName: String): Boolean {
        getAtlasConnection().use { conn ->
            conn.prepareStatement(checkIfDatabaseNameIsInUseSql).use { ps ->
                ps.setString(1, dbName)
                val rs = ps.executeQuery()

                return rs.next()
            }
        }
    }
}

val MEMBER_DATABASE_PERMISSIONS = setOf("CREATE", "CONNECT", "TEMPORARY", "TEMP")

internal fun createSchemaSql(schemaName: String): String {
    return "CREATE SCHEMA IF NOT EXISTS ${quote(schemaName)}"
}

internal fun dropSchemaSql(schemaName: String): String {
    return "DROP SCHEMA ${quote(schemaName)} CASCADE"
}

internal fun renameSchemaSql(oldName: String, newName: String): String {
    return "ALTER SCHEMA ${quote(oldName)} RENAME TO ${quote(newName)}"
}

internal fun grantUsageOnSchemaToRolesSql(schemaName: String, roles: Iterable<String>): String {
    return "GRANT USAGE ON SCHEMA ${quote(schemaName)} TO ${roles.joinToString { quote(it) }}"
}

internal fun revokeUsageOnSchemaToRolesSql(schemaName: String, roles: Iterable<String>): String {
    return "REVOKE USAGE ON SCHEMA ${quote(schemaName)} FROM ${roles.joinToString { quote(it) }}"
}

internal fun grantOrgUserPrivilegesOnSchemaSql(schemaName: Schemas, orgUserId: String): String {
    return "GRANT USAGE, CREATE ON SCHEMA ${quote(schemaName.label)} TO $orgUserId"
}

private fun setDefaultPrivilegesOnSchemaSql(schemaName: Schemas, usersSql: String): String {
    return "GRANT ALL PRIVILEGES ON SCHEMA ${quote(schemaName.label)} TO $usersSql"
}

private fun setAdminUserDefaultPrivilegesSql(schemaName: Schemas, usersSql: String): String {
    return "ALTER DEFAULT PRIVILEGES IN SCHEMA ${quote(schemaName.label)} GRANT ALL PRIVILEGES ON TABLES TO $usersSql"
}

private fun setSearchPathSql(granteeId: String, isUser: Boolean, vararg schemas: Schemas): String {
    val schemasSql = schemas.joinToString()
    val granteeType = if (isUser) "USER" else "ROLE"
    return "ALTER $granteeType $granteeId SET search_path TO $schemasSql"
}

private fun revokePrivilegesOnDatabaseSql(dbName: String, usersSql: String): String {
    return "REVOKE ${MEMBER_DATABASE_PERMISSIONS.joinToString(", ")} ON DATABASE ${quote(dbName)} FROM $usersSql"
}

private fun revokePrivilegesOnSchemaSql(schemaName: Schemas, usersSql: String): String {
    return "REVOKE ALL PRIVILEGES ON SCHEMA ${quote(schemaName.label)} FROM $usersSql"
}

private fun revokePrivilegesOnTablesInSchemaSql(schemaName: Schemas, usersSql: String): String {
    return "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA ${quote(schemaName.label)} FROM $usersSql"
}

internal fun createUserIfNotExistsSql(dbUser: String, dbUserPassword: String): String {
    return "DO\n" +
            "\$do\$\n" +
            "BEGIN\n" +
            "   IF NOT EXISTS (\n" +
            "      SELECT\n" +
            "      FROM   pg_catalog.pg_roles\n" +
            "      WHERE  rolname = '$dbUser') THEN\n" +
            "\n" +
            "      CREATE ROLE ${ApiHelpers.dbQuote(dbUser)} NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN ENCRYPTED PASSWORD '$dbUserPassword';\n" +
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

internal const val checkIfDatabaseNameIsInUseSql = "SELECT 1 FROM pg_database WHERE datname = ?"

internal const val renameDatabaseSql = "SELECT rename_database(?, ?)"

internal val createRenameDatabaseFunctionSql = """
    CREATE OR REPLACE FUNCTION rename_database(curr_name text, new_name text) RETURNS VOID AS $$
      BEGIN
        EXECUTE 'ALTER DATABASE ' || quote_ident(curr_name) || ' RENAME TO ' || quote_ident(new_name);
      END;
    $$ LANGUAGE plpgsql
""".trimIndent()

internal val createRenameServerDatabaseFunctionSql = """
    CREATE OR REPLACE FUNCTION $RENAME_SERVER_DB_FUNCTION(fdw_name text, new_db_name text) RETURNS VOID AS $$
      BEGIN
        EXECUTE 'ALTER SERVER ' || quote_ident(fdw_name) || ' OPTIONS (SET dbname ' || quote_literal(new_db_name) || ')';
      END;
    $$ LANGUAGE plpgsql
""".trimIndent()

internal const val databaseOidSql = "SELECT oid FROM pg_database WHERE datname = ?"
