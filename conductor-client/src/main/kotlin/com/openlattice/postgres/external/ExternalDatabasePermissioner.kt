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
import com.openlattice.edm.processors.GetSchemaFromExternalTableEntryProcessor
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.ExternalColumn
import com.openlattice.organization.ExternalTable
import com.openlattice.organization.roles.Role
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresPrivileges
import com.openlattice.postgres.TableColumn
import com.openlattice.postgres.mapstores.SecurableObjectTypeMapstore
import com.openlattice.transporter.grantUsageOnSchemaSql
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.sql.Connection
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
) : ExternalDatabasePermissioningService {

    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)
    private val securableObjectTypes = HazelcastMap.SECURABLE_OBJECT_TYPES.getMap(hazelcastInstance)
    private val externalColumns = HazelcastMap.EXTERNAL_COLUMNS.getMap(hazelcastInstance)
    private val externalTables = HazelcastMap.EXTERNAL_TABLES.getMap(hazelcastInstance)
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

    private fun getAtlasConnection(): Connection {
        return extDbManager.connectAsSuperuser().connection
    }

    override fun createRole(role: Role) {
        val (dbRole, _) = dbCredentialService.getOrCreateRoleAccount(role)

        logger.debug("Creating role if not exists {}", dbRole)
        getAtlasConnection().use { connection ->
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
        getAtlasConnection().use { connection ->
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

        // for ExternalColumns:
        val externalTableColAcls = aclsByType.getValue(SecurableObjectType.OrganizationExternalDatabaseColumn)
        executePrivilegesUpdateOnOrgExternalDbColumns(action, externalTableColAcls)
    }

    fun executePrivilegesUpdateOnOrgExternalDbColumns(
            action: Action,
            externalTableColAcls: List<Acl>
    ) {
        val extTableColIds = mutableSetOf<UUID>()
        val extTableIds = externalTableColAcls.mapTo(mutableSetOf()) {
            extTableColIds.add(it.aclKey[1])
            it.aclKey[0]
        }
        val extTablesById = externalTables.submitToKeys(extTableIds, GetSchemaFromExternalTableEntryProcessor()).toCompletableFuture().get().mapValues {
            Schemas.fromName(it.value)
        }

        val columnsById = externalColumns.getAll(extTableColIds).values.associate {
            AclKey(it.tableId, it.id) to TableColumn(it.organizationId, it.tableId, it.id, extTablesById[it.tableId])
        }
        updateExternalTablePermissions(action, externalTableColAcls, columnsById)
    }

    fun executePrivilegesUpdateOnPropertyTypes(action: Action, assemblyAcls: List<Acl>) {
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

    private fun groupAclsByType(acls: List<Acl>): Map<SecurableObjectType, List<Acl>> {
        val aclKeyIndex = acls.aclKeysAsSet { it.aclKey.index }.toTypedArray()
        return mapOf(
                getAclKeysOfSecurableObjectType(acls, aclKeyIndex, SecurableObjectType.OrganizationExternalDatabaseColumn),
                getAclKeysOfSecurableObjectType(acls, aclKeyIndex, SecurableObjectType.PropertyTypeInEntitySet)
        )
    }

    private fun getAclKeysOfSecurableObjectType(
            acls: List<Acl>,
            aclKeyIndex: Array<String>,
            securableObjectType: SecurableObjectType
    ): Pair<SecurableObjectType, List<Acl>> {
        val aclKeysOfType: Set<AclKey> = securableObjectTypes.keySet(
                getAclKeysOfObjectTypePredicate(aclKeyIndex, securableObjectType)
        )
        val aclsOfType = acls.filter { aclKeysOfType.contains(it.aclKey) }
        return securableObjectType to aclsOfType
    }

    private fun getAclKeysOfObjectTypePredicate(
            aclKeysIndexForm: Array<String>,
            objectType: SecurableObjectType
    ): Predicate<AclKey, SecurableObjectType> {
        return Predicates.and<AclKey, SecurableObjectType>(
                Predicates.`in`<Any, Any>(SecurableObjectTypeMapstore.ACL_KEY_INDEX, *aclKeysIndexForm),
                Predicates.equal<Any, Any>(SecurableObjectTypeMapstore.SECURABLE_OBJECT_TYPE_INDEX, objectType)
        )
    }

    private fun <T> Collection<Acl>.aclKeysAsSet(extraction: (Acl) -> T): Set<T> {
        return this.mapTo(HashSet(this.size)) { extraction(it) }
    }

    override fun addPrincipalToPrincipals(sourcePrincipalAclKey: AclKey, targetPrincipalAclKeys: Set<AclKey>) {
        val usernamesByAclKey = dbCredentialService.getDbUsernamesAsMap(targetPrincipalAclKeys + setOf(sourcePrincipalAclKey))

        val sourceRole = usernamesByAclKey[sourcePrincipalAclKey]
        if (sourceRole == null) {
            logger.info("Source principal {} has no corresponding role name in the database -- skipping db role grants", sourcePrincipalAclKey)
            return
        }

        val grantTargets = targetPrincipalAclKeys.filter { it != sourcePrincipalAclKey }.mapNotNull {
            val username = usernamesByAclKey[it]
            if (username == null) {
                logger.info("Target principal {} has no corresponding role name in the database. Skipping assignment to {}", it, sourcePrincipalAclKey)
            }
            username
        }.toSet()

        if (grantTargets.isEmpty()) {
            return
        }

        logger.debug("attempting to grant $sourceRole to $grantTargets")
        getAtlasConnection().use { connection ->
            connection.createStatement().use { stmt ->
                stmt.execute(grantRoleToRole(sourceRole, grantTargets))
            }
        }
    }

    override fun removePrincipalsFromPrincipals(principalsToRemove: Set<AclKey>, fromPrincipals: Set<AclKey>) {
        if (principalsToRemove.isEmpty() || fromPrincipals.isEmpty()) {
            return
        }
        
        val removeFrom = dbCredentialService.getDbUsernames(fromPrincipals)
        val removeSqls = dbCredentialService.getDbUsernames(principalsToRemove).map { roleName ->
            revokeRoleSql(roleName, removeFrom)
        }
        getAtlasConnection().use { connection ->
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
        val targets = propertyTypes.mapTo(mutableSetOf()) { (id, _) ->
            AccessTarget.forPermissionOnTarget(Permission.READ, entitySetId, id)
        }

        PostgresRoles.getOrCreatePermissionRolesAsync(
                externalRoleNames,
                targets,
                orgDatasource
        ).thenApplyAsync { targetsToRoleNames ->
            propertyTypes.map { (id, fqn) ->
                val quotedColumns = listOf(fqn, EdmConstants.ID_FQN).joinToString {
                    ApiHelpers.dbQuote(it.toString())
                }
                val permissions = olToPostgres.getValue(Permission.READ)
                val roleName = targetsToRoleNames[AccessTarget.forPermissionOnTarget(Permission.READ, entitySetId, id)].toString()
                grantUsageOnSchemaSql(Schemas.ASSEMBLED_ENTITY_SETS, roleName) to
                        grantPermissionsOnColumnsOnTableToRoleSql(
                                permissions,
                                quotedColumns,
                                Schemas.ASSEMBLED_ENTITY_SETS.label,
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
            table: ExternalTable,
            columns: Set<ExternalColumn>,
            adminRolePrincipal: Principal
    ) {
        initializePermissionSetForExternalTable(
                hikariDataSource = extDbManager.connectToOrg(organizationId),
                tableSchema = table.schema,
                tableName = table.name,
                columns = columns,
                principal = adminRolePrincipal,
                permissions = allTablePermissions

        )
    }

    /**
     * Create all postgres roles to apply to [table] and [columns] in [organizationId] database
     */
    override fun initializeProjectedTableViewPermissions(
            collaborationId: UUID,
            schema: String,
            table: ExternalTable,
            columns: Set<ExternalColumn>
    ) {
        initializePermissionSetForExternalTable(
                hikariDataSource = extDbManager.connectToOrg(collaborationId),
                tableSchema = schema,
                tableName = table.name,
                columns = columns,
                permissions = allViewPermissions

        )
    }

    private fun initializePermissionSetForExternalTable(
            hikariDataSource: HikariDataSource,
            tableSchema: String,
            tableName: String,
            columns: Set<ExternalColumn>,
            principal: Principal,
            permissions: Set<Permission>
    ) {
        val targetsToColumns = mutableMapOf<AccessTarget, ExternalColumn>()

        val targetsForSingularPrincipal = columns.flatMapTo(mutableSetOf()) { column ->
            permissions.map { permission ->
                val at = AccessTarget.forPermissionOnTarget(permission, column.tableId, column.id)
                targetsToColumns[at] = column
                at
            }
        }.associateWith {
            dbCredentialService.getDbUsername(principalsMapManager.getSecurablePrincipal(principal))
        }

        PostgresRoles.getOrCreatePermissionRolesAsync(
                externalRoleNames,
                targetsForSingularPrincipal,
                hikariDataSource
        ).thenApplyAsync { targetToRoleNames ->
            targetToRoleNames.map { (target, roleName) ->
                val column = targetsToColumns.getValue(target)
                val pgPermissions = olToPostgres.getValue(target.permission)
                grantPermissionsOnColumnsOnTableToRoleSql(
                        pgPermissions,
                        ApiHelpers.dbQuote(column.name),
                        tableSchema,
                        tableName,
                        roleName
                )
            }
        }.thenAccept { sqls ->
            hikariDataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    sqls.forEach { sql ->
                        stmt.addBatch(sql)
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

    override fun destroyExternalTablePermissions(organizationId: UUID, tablesToColumnIds: Map<UUID, Set<UUID>>) {
        val accessTargetsToDestroy = tablesToColumnIds.flatMap { (tableId, columnIds) ->
            columnIds.flatMap { columnId ->
                val aclKey = AclKey(tableId, columnId)
                allTablePermissions.map { permission -> AccessTarget(aclKey, permission) }
            }
        }.toSet()

        val permissionRoles = externalRoleNames.getAll(accessTargetsToDestroy)
        extDbManager.connectToOrg(organizationId).connection.use { conn ->
            conn.createStatement().use { stmt ->
                permissionRoles.forEach { (accessTarget, permissionRoleName) ->
                    try {
                        stmt.execute("DROP ROLE ${quote(permissionRoleName.toString())}")
                    } catch (e: Exception) {
                        logger.error("Unable to drop permission role {} for AccessTarget {}", permissionRoleName, accessTarget, e)
                    }
                }
            }
        }


        accessTargetsToDestroy.forEach {
            externalRoleNames.delete(it)
        }
    }

    private fun updateTablePermissions(
            action: Action,
            columnAcls: List<Acl>,
            columnsById: Map<AclKey, TableColumn>,
            tableType: TableType
    ) {

        when (action) {
            Action.ADD, Action.REMOVE, Action.SET -> {
            }
            else -> {
                logger.error("Action $action passed through to updateTablePermissions is unhandled. Doing no operations")
                return
            }
        }

        val allPermissions = if (tableType == TableType.TABLE) allTablePermissions else allViewPermissions

        val principalAclKeys = principalsMapManager.getAclKeyByPrincipal(
                columnAcls.flatMap { it.aces.map { a -> a.principal } }.toSet()
        )
        val usernamesByAclKey = dbCredentialService.getDbUsernamesAsMap(principalAclKeys.values.toSet())
        val principalToUsername = principalAclKeys.mapValues { usernamesByAclKey.getValue(it.value) }

        val removes = mutableMapOf<UUID, MutableList<String>>()
        val adds = mutableMapOf<UUID, MutableList<String>>()

        columnAcls.groupBy { columnsById.getValue(it.aclKey).organizationId }.forEach { (orgId, orgColumnAcls) ->
            val orgAdds = mutableListOf<String>()
            val orgRemoves = mutableListOf<String>()

            val accessTargetsToPermissionRoles = getPermissionRolesForAcls(orgId, orgColumnAcls, allPermissions)

            orgColumnAcls.forEach { columnAcl ->
                val column = columnsById.getValue(columnAcl.aclKey)

                columnAcl.aces.forEach { ace ->
                    val userRole = principalToUsername.getValue(ace.principal)
                    val requestedPermissionRoles = lookUpPermissionRoles(ace.permissions, columnAcl, accessTargetsToPermissionRoles)

                    when (action) {
                        Action.ADD -> {
                            orgAdds.addAll(grantPermissionsOnColumnSql(userRole, column, requestedPermissionRoles))
                        }
                        Action.REMOVE -> {
                            orgRemoves.addAll(removePermissionsOnColumn(userRole, requestedPermissionRoles))
                        }
                        Action.SET -> {
                            val allColPermissionRoles = lookUpPermissionRoles(allPermissions, columnAcl, accessTargetsToPermissionRoles)
                            orgRemoves.addAll(removePermissionsOnColumn(userRole, allColPermissionRoles))

                            orgAdds.addAll(grantPermissionsOnColumnSql(userRole, column, requestedPermissionRoles))
                        }
                        else -> {
                        }
                    }
                }
            }

            adds[orgId] = orgAdds
            removes[orgId] = orgRemoves
        }

        (adds.keys + removes.keys).forEach { organizationId ->
            val rems = removes.getOrDefault(organizationId, listOf<String>())
            val addz = adds.getOrDefault(organizationId, listOf<String>())
            extDbManager.connectToOrg(organizationId).connection.use { conn ->
                conn.autoCommit = false
                conn.createStatement().use { stmt ->
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
                        conn.rollback()
                    }
                }
            }
        }
    }

    private fun lookUpPermissionRoles(
            permissions: Set<Permission>,
            columnAcl: Acl,
            permissionRoles: Map<AccessTarget, UUID>
    ): List<UUID> {
        return permissions.mapNotNull { permissionRoles[AccessTarget(columnAcl.aclKey, it)] }
    }

    private fun getPermissionRolesForAcls(orgId: UUID, acls: List<Acl>, permissions: Set<Permission>): Map<AccessTarget, UUID> {
        val accessTargets = acls
                .map { it.aclKey }
                .flatMap {
                    permissions.map { permission -> AccessTarget(it, permission) }
                }.toSet()

        return PostgresRoles.getOrCreatePermissionRolesAsync(
                externalRoleNames,
                accessTargets,
                extDbManager.connectToOrg(orgId)
        ).toCompletableFuture().get()
    }

    private fun removePermissionsOnColumn(userRole: String, permissionRoles: List<UUID>): List<String> {
        return permissionRoles.map {
            revokeRoleSql(it.toString(), setOf(userRole))
        }
    }

    private fun grantPermissionsOnColumnsOnTableToRoleSql(
            privileges: Set<PostgresPrivileges>,
            columns: String,
            schemaName: String,
            tableName: String,
            roleName: String
    ): String {
        val privilegeString = privileges.joinToString { privilege ->
            "$privilege ( $columns )"
        }
        return """
            GRANT $privilegeString 
            ON $schemaName.${ApiHelpers.dbQuote(tableName)}
            TO ${ApiHelpers.dbQuote(roleName)}
        """.trimIndent()
    }

    private fun grantPermissionsOnColumnSql(
            userRole: String,
            column: TableColumn,
            permissionRoles: List<UUID>
    ): List<String> {
        val sqls = mutableListOf<String>()

        permissionRoles.forEach {
            sqls.add(grantRoleToRole(it.toString(), setOf(userRole)))

            if (column.schema != null) {
                sqls.add(grantUsageOnSchemaSql(column.schema, userRole))
            }
        }

        return sqls
    }

    private fun revokeRoleSql(roleName: String, targetRoles: Set<String>): String {
        return applyRoleOperation(roleName, targetRoles, PgPermAction.REVOKE)
    }

    private fun grantRoleToRole(roleName: String, targetRoles: Set<String>): String {
        return applyRoleOperation(roleName, targetRoles, PgPermAction.GRANT)
    }

    private fun applyRoleOperation(roleName: String, targetRoles: Set<String>, action: PgPermAction): String {
        val targets = targetRoles.joinToString {
            ApiHelpers.dbQuote(it)
        }
        return "${action.name} ${ApiHelpers.dbQuote(roleName)} ${action.verb} $targets"
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