package com.openlattice.postgres.external

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.ApiHelpers
import com.openlattice.assembler.PostgresRoles
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.edm.EdmConstants
import com.openlattice.edm.PropertyTypeIdFqn
import com.openlattice.edm.processors.GetOrganizationIdFromEntitySetEntryProcessor
import com.openlattice.edm.processors.GetSchemaFromOrganizationExternalTableEntryProcessor
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organization.roles.Role
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.PostgresPrivileges
import com.openlattice.postgres.TableColumn
import com.openlattice.postgres.mapstores.SecurableObjectTypeMapstore
import com.openlattice.transporter.grantUsageOnSchemaSql
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.sql.Statement
import java.util.*

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
@Component
class ExternalDatabasePermissioner(
        hazelcastInstance: HazelcastInstance,
        private val extDbManager: ExternalDatabaseConnectionManager,
        private val dbCredentialService: DbCredentialService,
        private val principalsMapManager: PrincipalsMapManager
): ExternalDatabasePermissioningService {
    private val atlas: HikariDataSource = extDbManager.connect("postgres")

    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)
    private val securableObjectTypes = HazelcastMap.SECURABLE_OBJECT_TYPES.getMap(hazelcastInstance)
    private val organizationExternalDatabaseColumns = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_COLUMN.getMap(hazelcastInstance)
    private val organizationExternalTables = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_TABLE.getMap(hazelcastInstance)
    private val externalRoleNames = HazelcastMap.EXTERNAL_PERMISSION_ROLES.getMap(hazelcastInstance)

    companion object {
        private val logger = LoggerFactory.getLogger(ExternalDatabasePermissioner::class.java)

        private val olToPostgres = mapOf<Permission, Set<PostgresPrivileges>>(
                Permission.READ to EnumSet.of(PostgresPrivileges.SELECT),
                Permission.WRITE to EnumSet.of(PostgresPrivileges.INSERT, PostgresPrivileges.UPDATE),
                Permission.OWNER to EnumSet.of(PostgresPrivileges.ALL)
        )

        private val allViewPermissions = setOf(Permission.READ)

        private val allTablePermissions = setOf(Permission.READ, Permission.WRITE, Permission.OWNER)
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

    override fun executePrivilegesUpdate(action: Action, acls: List<Acl>) {
        val aclsByType = groupAclsByType(acls)

        // for entityset aclkeys:
        val assemblyCols = aclsByType.getValue(SecurableObjectType.PropertyTypeInEntitySet)
        executePrivilegesUpdateOnPropertyTypes(action, assemblyCols)

        // for organizationexternalDatabaseColumns:
        val externalTableColAcls = aclsByType.getValue(SecurableObjectType.OrganizationExternalDatabaseColumn)
        val externalTables = aclsByType.getValue(SecurableObjectType.OrganizationExternalDatabaseTable)
        executePrivilegesUpdateOnOrgExternalDbColumns(action, externalTableColAcls, externalTables)
    }

    fun executePrivilegesUpdateOnOrgExternalDbColumns(
            action: Action,
            externalTableColAcls: List<Acl>,
            externalTableAcls: List<Acl>
    ) {
        val extTableColIds = externalTableColAcls.aclKeysAsSet {
            it.aclKey[1]
        }

        val extTableIds = externalTableAcls.aclKeysAsSet {
            it.aclKey[1]
        }
        val extTablesById = organizationExternalTables.submitToKeys(extTableIds, GetSchemaFromOrganizationExternalTableEntryProcessor()).toCompletableFuture().get().mapValues {
            Schemas.valueOf(it.value)
        }

        val columnsById = organizationExternalDatabaseColumns.getAll(extTableColIds).values.associate {
            AclKey( it.tableId, it.id) to TableColumn(it.organizationId, it.tableId, it.id, extTablesById[it.tableId])
        }
        updateExternalTablePermissions(action, externalTableColAcls, columnsById)
    }

    fun executePrivilegesUpdateOnPropertyTypes(action: Action, assemblyAcls: List<Acl> ){
        val esids = assemblyAcls.aclKeysAsSet {
            it.aclKey[0]
        }
        val aclKeyToTableCols = entitySets.submitToKeys(esids, GetOrganizationIdFromEntitySetEntryProcessor()).thenApplyAsync { esidToOrgId ->
            assemblyAcls.associate {
                it.aclKey to TableColumn(esidToOrgId.getValue(it.aclKey[0]), it.aclKey[0], it.aclKey[1])
            }
        }
        updateAssemblyPermissions(action, assemblyAcls, aclKeyToTableCols.toCompletableFuture().get())
    }

    private fun groupAclsByType(acls: List<Acl>): Map<SecurableObjectType, List<Acl>>{
        val aclKeyIndex = acls.aclKeysAsSet { it.aclKey.index }.toTypedArray()
        return mapOf(
                getAclKeysOfSecurableObjectType(acls, aclKeyIndex, SecurableObjectType.OrganizationExternalDatabaseColumn),
                getAclKeysOfSecurableObjectType(acls, aclKeyIndex, SecurableObjectType.PropertyTypeInEntitySet),
                getAclKeysOfSecurableObjectType(acls, aclKeyIndex, SecurableObjectType.OrganizationExternalDatabaseTable)
        )
    }

    private fun getAclKeysOfSecurableObjectType(
            acls: List<Acl>,
            aclKeyIndex: Array<String>,
            securableObjectType: SecurableObjectType
    ): Pair<SecurableObjectType, List<Acl>> {
        val aclKeysOfType: Set<AclKey> = securableObjectTypes.keySet(
                getAclKeysOfObjectTypePredicate( aclKeyIndex, securableObjectType)
        )
        val aclsOfType = acls.filter { aclKeysOfType.contains(it.aclKey) }
        return SecurableObjectType.OrganizationExternalDatabaseTable to aclsOfType
    }

    private fun getAclKeysOfObjectTypePredicate(
            aclKeysIndexForm: Array<String>,
            objectType: SecurableObjectType
    ): Predicate<AclKey, SecurableObjectType> {
        return Predicates.and<AclKey, SecurableObjectType>(
                Predicates.`in`<Any, Any>(SecurableObjectTypeMapstore.ACL_KEY_INDEX, *aclKeysIndexForm),
                Predicates.equal<Any, Any>(SecurableObjectTypeMapstore.SECURABLE_OBJECT_TYPE_INDEX, objectType )
        )
    }

    private fun <T> Collection<Acl>.aclKeysAsSet( extraction: ( Acl ) -> T ): Set<T> {
        return this.mapTo( HashSet(this.size) ) { extraction(it) }
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
        val removeFrom = dbCredentialService.getDbUsernames(fromPrincipals)
        val removeSqls = dbCredentialService.getDbUsernames(principalsToRemove).map { roleName ->
            revokeRoleSql(roleName, removeFrom)
        }
        atlas.connection.use { connection ->
            connection.createStatement().use { stmt ->
                removeSqls.forEach { sql ->
                    stmt.addBatch(sql)
                }
                stmt.executeBatch()
            }
        }
    }

    /**
     * Create all postgres roles to apply to [entitySetId] and [propertyTypes]
     * Adds permissions on [EdmConstants.ID_FQN] to each of the above roles
     */
    override fun initializeAssemblyPermissions(
            orgDatasource: HikariDataSource,
            entitySetId: UUID,
            entitySetName: String,
            propertyTypes: Set<PropertyTypeIdFqn>
    ) {
        val targets = propertyTypes.mapTo(mutableSetOf()) { (id, fqn) ->
            AccessTarget.forPermissionOnTarget(Permission.READ, entitySetId, id)
        }

        PostgresRoles.getOrCreatePermissionRolesAsync( externalRoleNames, targets ) { roleName ->
            orgDatasource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute(createRoleIfNotExistsSql(roleName))
                }
            }
        }.thenApplyAsync { targetsToRoleNames ->
            propertyTypes.map { (id, fqn) ->
                val quotedColumns = listOf(fqn, EdmConstants.ID_FQN).joinToString {
                    ApiHelpers.dbQuote(it.toString())
                }
                val permissions = olToPostgres[Permission.READ]!!.joinToString()
                val roleName = targetsToRoleNames[AccessTarget.forPermissionOnTarget(Permission.READ, entitySetId, id)].toString()
                grantUsageOnSchemaSql(Schemas.ASSEMBLED_ENTITY_SETS, roleName) to
                        grantPermissionsOnColumnsOnTableToRoleSql(
                                permissions,
                                quotedColumns,
                                Schemas.ASSEMBLED_ENTITY_SETS,
                                entitySetName,
                                roleName)
            }
        }.thenAccept { ptToSqls ->
            orgDatasource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    ptToSqls.forEach { (grantSchemaSql, grantViewSql) ->
                        stmt.execute(grantSchemaSql)
                        stmt.execute(grantViewSql)
                    }
                }
            }
        }
    }

    /**
     * Updates permissions on [propertyTypes] for [entitySet] in org database for [organizationId]
     */
    override fun updateAssemblyPermissions(
            action: Action,
            columnAcls: List<Acl>,
            columnsById: Map<AclKey, TableColumn>
    ) {
        updateTablePermissions(action, columnAcls, columnsById, TableType.VIEW)
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
                val roleName = ""//PostgresRoles.getOrCreatePermissionRole( externalRoleNames, permission, table.id, col.id)
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
            action: Action,
            columnAcls: List<Acl>,
            columnsById: Map<AclKey, TableColumn>
    ) {
        updateTablePermissions(action, columnAcls, columnsById, TableType.TABLE)
    }

    private fun updateTablePermissions(
            action: Action,
            columnAcls: List<Acl>,
            columnsById: Map<AclKey, TableColumn>,
            tableType: TableType
    ) {
        if (action != Action.ADD && action != Action.REMOVE && action != Action.SET) {
            logger.error("Action $action passed through to updateTablePermissions is unhandled. Doing no operations")
            return
        }

        val allOrgs = columnsById.values.map { it.organizationId }.toSet()
        val removes = mutableMapOf<UUID, MutableList<String>>()
        val adds = mutableMapOf<UUID, MutableList<String>>()
        columnAcls.forEach { columnAcl ->
            val column = columnsById.getValue(columnAcl.aclKey)
            val orgId = column.organizationId
            columnAcl.aces.forEach { ace ->
                when (action) {
                    Action.ADD -> {
                        val sqls = adds.getOrDefault(orgId, mutableListOf())
                        sqls.addAll( updatePermissionsOnColumnSql(
                                orgId,
                                ace,
                                column,
                                PgPermAction.GRANT
                        ) )
                        adds[orgId] = sqls
                    }
                    Action.REMOVE -> {
                        val sqls = removes.getOrDefault(orgId, mutableListOf())
                        sqls.addAll(
                                removeAllPermissionsForPrincipalOnColumn(
                                        orgId, ace.principal, column, tableType
                                )
                        )
                        removes[orgId] = sqls
                    }
                    Action.SET -> {
                        val remSqls = removes.getOrDefault(orgId, mutableListOf())
                        remSqls.addAll(
                                removeAllPermissionsForPrincipalOnColumn(
                                        orgId, ace.principal, column, tableType
                                )
                        )
                        removes[orgId] = remSqls

                        val addSqls = adds.getOrDefault(orgId, mutableListOf())
                        addSqls.addAll( updatePermissionsOnColumnSql(
                                orgId,
                                ace,
                                column,
                                PgPermAction.GRANT
                        ))
                        adds[orgId] = addSqls
                    }
                }
            }
        }

        allOrgs.forEach { organizationId ->
            val rems = removes.getOrDefault(organizationId, listOf<String>())
            val addz = adds.getOrDefault(organizationId, listOf<String>())
            extDbManager.connectToOrg(organizationId).connection.use { conn ->
                conn.autoCommit = false
                val stmt: Statement = conn.createStatement()
                try {
                    rems.forEach { sql ->
                        stmt.addBatch(sql)
                    }
                    stmt.executeBatch()
                    stmt.clearBatch()

                    addz.forEach { sql ->
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
    }

    private fun removeAllPermissionsForPrincipalOnColumn(
            orgId: UUID,
            principal: Principal,
            column: TableColumn,
            viewOrTable: TableType
    ): List<String> {
        val targetAclKey = AclKey(column.tableId, column.columnId)
        val targets = when (viewOrTable) {
            TableType.VIEW -> allViewPermissions.mapTo(mutableSetOf<AccessTarget>()) { AccessTarget(targetAclKey, it) }
            TableType.TABLE -> allTablePermissions.mapTo(mutableSetOf<AccessTarget>()) { AccessTarget(targetAclKey, it) }
        }

        val securablePrincipal = principalsMapManager.getSecurablePrincipal(principal.id)
        val userRole = dbCredentialService.getDbUsername(securablePrincipal)

        return PostgresRoles.getOrCreatePermissionRolesAsync( externalRoleNames, targets ) { roleName ->
            extDbManager.connectToOrg(orgId).connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute(createRoleIfNotExistsSql(roleName))
                }
            }
        }.thenApply {
            it.values.map { permissionRoleName ->
                revokeRoleSql(permissionRoleName.toString(), setOf(userRole))
            }
        }.toCompletableFuture().get()
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

    private fun updatePermissionsOnColumnSql(
            orgId: UUID,
            ace: Ace,
            column: TableColumn,
            action: PgPermAction
    ): List<String> {
        val accessTargets = filteredAcePermissions(ace.permissions).mapTo(mutableSetOf()) { perm ->
            AccessTarget(AclKey(column.tableId, column.columnId), perm)
        }
        val securablePrincipal = principalsMapManager.getSecurablePrincipal(ace.principal.id)
        val usernameAsync = dbCredentialService.getDbUsernameAsync(securablePrincipal)

        val permissionRoles = PostgresRoles.getOrCreatePermissionRolesAsync( externalRoleNames, accessTargets ) { roleName ->
            extDbManager.connectToOrg(orgId).connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute(createRoleIfNotExistsSql(roleName))
                }
            }
        }.thenCombine( usernameAsync ) { targetsToNames, userRole ->
            val sqls = mutableListOf<String>()
            targetsToNames.values.forEach { permissionRole ->
                when (action) {
                    PgPermAction.GRANT -> sqls.add(grantRoleToRole(permissionRole.toString(), setOf(userRole)))
                    PgPermAction.REVOKE -> sqls.add(revokeRoleSql(permissionRole.toString(), setOf(userRole)))
                }
            }
            if (column.schema != null){
                sqls.add(grantUsageOnSchemaSql(column.schema, userRole))
            }
            sqls
        }.toCompletableFuture()

        return permissionRoles.get()
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
                "      CREATE ROLE ${ApiHelpers.dbQuote(dbUser)} NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN ENCRYPTED PASSWORD '$dbUserPassword';\n" +
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