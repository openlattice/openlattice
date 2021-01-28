package com.openlattice.postgres.external

import com.openlattice.ApiHelpers
import com.openlattice.assembler.PostgresRoles
import com.openlattice.authorization.*
import com.openlattice.edm.EdmConstants
import com.openlattice.edm.PropertyTypeIdFqn
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organization.roles.Role
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.PostgresPrivileges
import com.openlattice.postgres.TableColumn
import com.openlattice.transporter.grantUsageOnschemaSql
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.sql.Statement
import java.util.*

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
@Component
class ExternalDatabasePermissioner(
        private val extDbManager: ExternalDatabaseConnectionManager,
        private val dbCredentialService: DbCredentialService,
        private val principalsMapManager: PrincipalsMapManager
): ExternalDatabasePermissioningService {
    private val atlas: HikariDataSource = extDbManager.connect("postgres")

    companion object {
        private val logger = LoggerFactory.getLogger(ExternalDatabasePermissioner::class.java)

        private val olToPostgres = mapOf<Permission, Set<PostgresPrivileges>>(
                Permission.READ to EnumSet.of(PostgresPrivileges.SELECT),
                Permission.WRITE to EnumSet.of(PostgresPrivileges.INSERT, PostgresPrivileges.UPDATE),
                Permission.OWNER to EnumSet.of(PostgresPrivileges.ALL)
        )

        private val allViewPermissions = setOf(Permission.READ)

        private val allTablePermissions = setOf(Permission.READ, Permission.WRITE, Permission.OWNER)

        private fun fqnToColumnName(fqn: FullQualifiedName): String {
            return fqn.toString()
        }
    }

    internal fun configureRolesInDatabase(dataSource: HikariDataSource) {
        val roles = principalsMapManager.getAllRoles()

        if (roles.isNotEmpty()) {
            val roleIds = roles.map { PostgresRoles.buildPostgresRoleName(it.id) }
            val roleIdsSql = roleIds.joinToString { DataTables.quote(it) }

            dataSource.connection.use { connection ->
                connection.createStatement().use { statement ->

                    logger.info("Revoking ${Schemas.PUBLIC_SCHEMA} schema right from roles: {}", roleIds)
                    //Don't allow users to access public schema which will contain foreign data wrapper tables.
                    statement.execute("REVOKE USAGE ON SCHEMA ${Schemas.PUBLIC_SCHEMA} FROM $roleIdsSql")
                }
            }
        }
    }

    override fun createRole(role: Role) {
        val (dbRole, _) = dbCredentialService.getOrCreateRoleAccount(role)

        logger.debug("Creating role if not exists {}", dbRole)
        atlas.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(createRoleIfNotExistsSql(dbRole))
                //Don't allow users to access public schema which will contain foreign data wrapper tables.
                logger.debug("Revoking ${Schemas.PUBLIC_SCHEMA} schema right from role: {}", role)
                statement.execute("REVOKE USAGE ON SCHEMA ${Schemas.PUBLIC_SCHEMA} FROM ${DataTables.quote(dbRole)}")
            }
        }
    }

    override fun createUnprivilegedUser(principal: SecurablePrincipal) {
        /**
         * To simplify work-around for ESRI username limitations, we are only introducing one additional
         * field into the dbcreds table. We keep the results of calling [buildPostgresUsername] as the lookup
         * key, but instead use the username and password returned from the db credential service.
         */
        val (dbUser, dbUserPassword) = dbCredentialService.getOrCreateDbAccount(principal)

        logger.debug("Creating user if not exists {}", dbUser)
        atlas.connection.use { connection ->
            connection.createStatement().use { statement ->
                //TODO: Go through every database and for old users clean them out.
//                    logger.info("Attempting to drop owned by old name {}", user.name)
//                    statement.execute(dropOwnedIfExistsSql(user.name))
//                    logger.info("Attempting to drop user {}", user.name)
//                    statement.execute(dropUserIfExistsSql(user.name)) //Clean out the old users.
//                    logger.info("Creating new user {}", dbUser)
                statement.execute(createUserIfNotExistsSql(dbUser, dbUserPassword))
                //Don't allow users to access public schema which will contain foreign data wrapper tables.
                logger.debug("Revoking ${Schemas.PUBLIC_SCHEMA} schema right from user {}", principal)
                statement.execute("REVOKE USAGE ON SCHEMA ${Schemas.PUBLIC_SCHEMA} FROM ${DataTables.quote(dbUser)}")
            }
        }
    }

    override fun addPrincipalToPrincipals(sourcePrincipalAclKey: AclKey, targetPrincipalAclKeys: Set<AclKey>) {
        if ( targetPrincipalAclKeys.isEmpty()) {
            return
        }
        val grantTargets = dbCredentialService.getDbUsernames(targetPrincipalAclKeys)

        val sourceRole = dbCredentialService.getDbUsername(sourcePrincipalAclKey)

        logger.debug("attempting to grant $sourceRole to $grantTargets")
        atlas.connection.use { connection ->
            connection.createStatement().use { stmt ->
                stmt.execute(grantRoleToRole(sourceRole, grantTargets))
            }
        }
    }

    override fun removePrincipalsFromPrincipals(principalsToRemove: Set<AclKey>, fromPrincipals: Set<AclKey>) {
        val removeFrom = principalsToPostgresRoleNames(fromPrincipals)
        atlas.connection.use { connection ->
            connection.createStatement().use { stmt ->
                principalsToRemove.forEach { aclKey ->
                    val roleName = PostgresRoles.buildPostgresRoleName(principalsMapManager.lookupRole(aclKey).id)
                    stmt.addBatch(revokeRoleSql(roleName, removeFrom))
                }
                stmt.executeBatch()
            }
        }
    }

    private fun principalsToPostgresRoleNames(principals: Set<AclKey>): Set<String> {
        return dbCredentialService.getDbUsernames(principals)
    }

    /**
     * Create all postgres roles to apply to [entitySet] and [propertyTypes]
     * Adds permissions on [EdmConstants.ID_FQN] to each of the above roles
     */
    override fun initializeAssemblyPermissions(
            orgDatasource: HikariDataSource,
            entitySetId: UUID,
            entitySetName: String,
            propertyTypes: Set<PropertyTypeIdFqn>
    ) {
        val ptToSqls = propertyTypes.map { (id, fqn) ->
            val roleName = PostgresRoles.buildPermissionRoleName(entitySetId, id, Permission.READ)
            val quotedColumns = listOf(fqn, EdmConstants.ID_FQN).joinToString {
                ApiHelpers.dbQuote(fqnToColumnName(it))
            }
            val permissions = olToPostgres[Permission.READ]!!.joinToString()
            // roleName to listOf(
            //   createRoleIfNotExistsSql(roleName),
            //   grantPermissionsOnColumnsOnTableToRoleSql()
            Triple(
                    createRoleIfNotExistsSql(roleName),
                    grantUsageOnschemaSql(Schemas.ASSEMBLED_ENTITY_SETS, roleName),
                    grantPermissionsOnColumnsOnTableToRoleSql(
                            permissions,
                            quotedColumns,
                            Schemas.ASSEMBLED_ENTITY_SETS,
                            entitySetName,
                            roleName)
            )
        }

        orgDatasource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                ptToSqls.forEach { (createRoleSql, grantSchemaSql, grantViewSql) ->
                    stmt.addBatch(createRoleSql)
                    stmt.addBatch(grantSchemaSql)
                    stmt.addBatch(grantViewSql)
                }
                stmt.executeBatch()
            }
        }
    }

    /**
     * Updates permissions on [propertyTypes] for [entitySet] in org database for [organizationId]
     */
    override fun updateAssemblyPermissions(
            organizationId: UUID,
            action: Action,
            columnAcls: List<Acl>,
            columnsById: Map<UUID, TableColumn>
    ) {
        updateTablePermissions(organizationId, action, columnAcls, columnsById, TableType.VIEW)
    }

    /**
     * Create all postgres roles to apply to [table] and [columns] in [organizationId] database
     */
    override fun initializeExternalTablePermissions(
            organizationId: UUID,
            table: OrganizationExternalDatabaseTable,
            columns: Set<OrganizationExternalDatabaseColumn>
    ) {
        val sqlCommandsPerPropertyType = columns.map { col ->
            val quotedColumn = ApiHelpers.dbQuote(col.name)
            olToPostgres.map { (permission, pgPermission) ->
                val roleName = PostgresRoles.buildPermissionRoleName(table.id, col.id, permission)
                val pgPermissionString = pgPermission.joinToString()
                createRoleIfNotExistsSql(roleName) to grantPermissionsOnColumnsOnTableToRoleSql(
                        pgPermissionString,
                        quotedColumn,
                        null,
                        table.name,
                        roleName
                )
            }
        }

        extDbManager.connectToOrg(organizationId).connection.use { conn ->
            conn.createStatement().use { stmt ->
                sqlCommandsPerPropertyType.forEach { createSqlToGrantSql ->
                    createSqlToGrantSql.map { (createRoleSql, grantSql) ->
                        stmt.addBatch(createRoleSql)
                        stmt.addBatch(grantSql)
                    }
                    stmt.executeBatch()
                }
            }
        }
    }

    /**
     * Updates permissions on [columns] for [table] in org database for [organizationId]
     */
    override fun updateExternalTablePermissions(
            organizationId: UUID,
            action: Action,
            columnAcls: List<Acl>,
            columnsById: Map<UUID, OrganizationExternalDatabaseColumn>
    ) {
//        updateTablePermissions(organizationId, action, columnAcls, columnsById, TableType.TABLE)
    }

    private fun updateTablePermissions(
            organizationId: UUID,
            action: Action,
            columnAcls: List<Acl>,
            columnsById: Map<UUID, TableColumn>,
            tableType: TableType
    ) {
        if (action != Action.ADD && action != Action.REMOVE && action != Action.SET) {
            logger.error("Action $action passed through to updateTablePermissions is unhandled. Doing no operations")
            return
        }

        val removes = mutableSetOf<String>()
        val adds = mutableSetOf<String>()
        columnAcls.forEach { columnAcl ->
            val column = columnsById.getValue(columnAcl.aclKey.last())
            columnAcl.aces.forEach { ace ->
                when (action) {
                    Action.ADD -> {
                        adds.addAll(updatePermissionsOnColumnSql(
                                ace,
                                column,
                                PgPermAction.GRANT
                        ))
                    }
                    Action.REMOVE -> {
                        removes.addAll(updatePermissionsOnColumnSql(
                                ace,
                                column,
                                PgPermAction.REVOKE
                        ))
                    }
                    Action.SET -> {
                        removes.addAll(removeAllPermissionsForPrincipalOnColumn(ace.principal, column, tableType))
                        adds.addAll(updatePermissionsOnColumnSql(
                                ace,
                                column,
                                PgPermAction.GRANT
                        ))
                    }
                }
            }
        }

        extDbManager.connectToOrg(organizationId).connection.use { conn ->
            conn.autoCommit = false
            val stmt: Statement = conn.createStatement()
            try {
                removes.forEach {
                    stmt.execute(it)
                }
                adds.forEach { sql ->
                    stmt.addBatch(sql)
                }
                stmt.executeBatch()
                conn.commit()
            } catch (ex: Exception) {
                logger.error("Exception occurred during external permissions update, rolling back", ex)
            } finally {
                conn.rollback()
                stmt.close()
            }
        }
    }

    private fun removeAllPermissionsForPrincipalOnColumn(
            principal: Principal,
            column: TableColumn,
            viewOrTable: TableType
    ): List<String> {
        val securablePrincipal = principalsMapManager.getSecurablePrincipal(principal.id)
        val userRole = dbCredentialService.getDbUsername(securablePrincipal)
        return when (viewOrTable) {
            TableType.VIEW -> allViewPermissions
            TableType.TABLE -> allTablePermissions
        }.map {
            revokeRoleSql(PostgresRoles.buildPermissionRoleName(column.tableId, column.id, it), setOf(userRole))
        }
    }

    private fun grantPermissionsOnColumnsOnTableToRoleSql(
            permissions: String,
            columns: String,
            schemaName: Schemas?,
            tableName: String,
            roleName: String): String {
        val schema = schemaName ?: ""
        return """
            GRANT $permissions ( $columns )
            ON $schema.${ApiHelpers.dbQuote(tableName)}
            TO ${ApiHelpers.dbQuote(roleName)}
        """.trimIndent()
    }

    private fun updatePermissionsOnColumnSql(ace: Ace, column: TableColumn, action: PgPermAction): List<String> {
        val securablePrincipal = principalsMapManager.getSecurablePrincipal(ace.principal.id)
        val userRole = dbCredentialService.getDbUsername(securablePrincipal)
        return filteredAcePermissions(ace.permissions).map { perm ->
            val permissionsRole = PostgresRoles.buildPermissionRoleName(column.tableId, column.id, perm)
            when (action) {
                PgPermAction.GRANT -> grantRoleToRole(permissionsRole, setOf(userRole))
                PgPermAction.REVOKE -> revokeRoleSql(permissionsRole, setOf(userRole))
            }
        }
    }

    private fun revokeRoleSql(roleName: String, targetRoles: Set<String>): String {
        return applyRoleOperation(roleName, targetRoles, PgPermAction.REVOKE)
    }

    private fun grantRoleToRole(roleName: String, targetRoles: Set<String>): String {
        return applyRoleOperation(roleName, targetRoles, PgPermAction.GRANT)
    }

    private fun applyRoleOperation(roleName: String, targetRoles: Set<String>, action: PgPermAction ): String {
        val targets = targetRoles.joinToString {
            ApiHelpers.dbQuote(it)
        }
        return "${action.name} ${ApiHelpers.dbQuote(roleName)} ${action.verb} $targets"
    }

    private fun filteredAcePermissions(permissions: Set<Permission>): Set<Permission> {
        return permissions.filter { olToPostgres.keys.contains(it) }.toSet()
    }

    internal fun createRoleIfNotExistsSql(dbRole: String): String {
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

    private enum class PgPermAction(val verb: String) {
        GRANT("TO"), REVOKE("FROM")
    }

    private enum class TableType {
        VIEW, TABLE
    }

}