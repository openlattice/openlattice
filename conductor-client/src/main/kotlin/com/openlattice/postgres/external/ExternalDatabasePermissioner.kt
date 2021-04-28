package com.openlattice.postgres.external

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.ApiHelpers
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.edm.EdmConstants
import com.openlattice.edm.processors.GetFqnFromPropertyTypeEntryProcessor
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.roles.Role
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.PostgresPrivileges
import com.openlattice.postgres.TableColumn
import com.openlattice.postgres.mapstores.SecurableObjectTypeMapstore
import com.openlattice.transporter.grantUsageOnSchemaSql
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
        private val principalsMapManager: PrincipalsMapManager,
        private val principalPermissionQueryService: PrincipalPermissionQueryService
) : ExternalDatabasePermissioningService {

    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)
    private val propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap(hazelcastInstance)
    private val securableObjectTypes = HazelcastMap.SECURABLE_OBJECT_TYPES.getMap(hazelcastInstance)
    private val externalColumns = HazelcastMap.EXTERNAL_COLUMNS.getMap(hazelcastInstance)
    private val externalTables = HazelcastMap.EXTERNAL_TABLES.getMap(hazelcastInstance)

    companion object {
        private val logger = LoggerFactory.getLogger(ExternalDatabasePermissioner::class.java)

        private val olToPostgres = mapOf<Permission, Set<PostgresPrivileges>>(
                Permission.READ to EnumSet.of(PostgresPrivileges.SELECT),
                Permission.WRITE to EnumSet.of(PostgresPrivileges.INSERT, PostgresPrivileges.UPDATE),
                Permission.OWNER to EnumSet.of(PostgresPrivileges.REFERENCES)
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

        val externalTablesById = externalTables.getAll(extTableIds)

        val columnsById = externalColumns.getAll(extTableColIds).values.associate {
            val table = externalTablesById.getValue(it.tableId)
            AclKey(it.tableId, it.id) to TableColumn(
                    it.organizationId,
                    it.tableId,
                    it.id,
                    Schemas.fromName(table.schema),
                    it.name,
                    table.name
            )
        }
        updateExternalTablePermissions(action, externalTableColAcls, columnsById)
    }

    fun executePrivilegesUpdateOnPropertyTypes(action: Action, assemblyAcls: List<Acl>) {
        val esIds = assemblyAcls.aclKeysAsSet { it.aclKey[0] }
        val ptIds = assemblyAcls.aclKeysAsSet { it.aclKey[1] }

        val entitySetsById = entitySets.getAll(esIds)
        val propertyTypesById = propertyTypes.executeOnKeys(ptIds, GetFqnFromPropertyTypeEntryProcessor())

        val aclKeyToTableCols = assemblyAcls.associate {
            val entitySet = entitySetsById.getValue(it.aclKey[0])
            it.aclKey to TableColumn(
                    entitySet.organizationId,
                    it.aclKey[0],
                    it.aclKey[1],
                    Schemas.ASSEMBLED_ENTITY_SETS,
                    entitySet.name,
                    propertyTypesById.getValue(it.aclKey[1]).fullQualifiedNameAsString
            )

        }
        updateAssemblyPermissions(action, assemblyAcls, aclKeyToTableCols)
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

    private fun handlePrincipalsChange(principalAddedOrRemoved: AclKey, targetPrincipals: Set<AclKey>, action: Action) {
        val aceKeyToPermissions = principalPermissionQueryService.resolvePermissionChangeOnPrincipalTreeChange(
                principalAddedOrRemoved,
                targetPrincipals
        )

        val aclKeyTypes = securableObjectTypes.getAll(aceKeyToPermissions.keys.map { it.aclKey }.toSet())
                .entries.groupBy { it.value }.mapValues { it.value.mapTo(mutableSetOf()) { e -> e.key } }

        // update permissions for property types on entity sets
        aclKeyTypes[SecurableObjectType.PropertyTypeInEntitySet]?.let { ptAclKeys ->
            val ptUpdates = aceKeyToPermissions.filter { ptAclKeys.contains(it.key.aclKey) }.entries.groupBy {
                it.key.aclKey
            }.mapValues { it.value }

            val tablesById = entitySets.getAll(ptUpdates.keys.mapTo(mutableSetOf()) { it.first() }).filter {
                it.value.flags.contains(EntitySetFlag.TRANSPORTED)
            }
            val columnsById = propertyTypes.getAll(ptUpdates.keys.mapTo(mutableSetOf()) { it.last() })

            val aclUpdates = ptUpdates.filter { tablesById.containsKey(it.key.first()) }.map { (aclKey, permissionUpdates) ->
                Acl(aclKey, permissionUpdates.map { Ace(it.key.principal, it.value) })
            }

            val tableColumns = ptUpdates.keys.filter { tablesById.containsKey(it.first()) }.associateWith {
                val col = columnsById.getValue(it.last())
                val table = tablesById.getValue(it.first())
                TableColumn(
                        table.organizationId,
                        table.id,
                        col.id,
                        Schemas.ASSEMBLED_ENTITY_SETS,
                        col.type.fullQualifiedNameAsString,
                        table.name
                )
            }

            updateTablePermissions(
                    action,
                    aclUpdates,
                    tableColumns,
                    TableType.VIEW,
                    isAssembledEntitySet = true
            )
        }

        // update permissions for columns on external tables
        aclKeyTypes[SecurableObjectType.OrganizationExternalDatabaseColumn]?.let { columnAclKeys ->
            val colUpdates = aceKeyToPermissions.filter { columnAclKeys.contains(it.key.aclKey) }.entries.groupBy {
                it.key.aclKey
            }.mapValues { it.value }

            val tablesById = externalTables.getAll(colUpdates.keys.mapTo(mutableSetOf()) { it.first() })
            val columnsById = externalColumns.getAll(colUpdates.keys.mapTo(mutableSetOf()) { it.last() })

            val aclUpdates = colUpdates.map { (aclKey, permissionUpdates) ->
                Acl(aclKey, permissionUpdates.map { Ace(it.key.principal, it.value) })
            }

            val tableColumns = colUpdates.keys.filter { tablesById.containsKey(it.first()) }.associateWith {
                val col = columnsById.getValue(it.last())
                val table = tablesById.getValue(it.first())
                TableColumn(
                        table.organizationId,
                        table.id,
                        col.id,
                        Schemas.fromName(table.schema),
                        col.name,
                        table.name
                )
            }

            updateTablePermissions(
                    action,
                    aclUpdates,
                    tableColumns,
                    TableType.TABLE
            )
        }
    }

    override fun addPrincipalToPrincipals(sourcePrincipalAclKey: AclKey, targetPrincipalAclKeys: Set<AclKey>) {
        handlePrincipalsChange(sourcePrincipalAclKey, targetPrincipalAclKeys, Action.ADD)
    }

    override fun removePrincipalsFromPrincipals(principalsToRemove: Set<AclKey>, fromPrincipals: Set<AclKey>) {
        principalsToRemove.forEach {
            handlePrincipalsChange(it, fromPrincipals, Action.REMOVE)
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
        if (!actionTypeIsValid(action)) {
            return
        }

        updateTablePermissions(action, columnAcls, columnsById, TableType.VIEW, true)
    }

    /**
     * Updates permissions on [columns] for [table] in org database for [organizationId]
     */
    override fun updateExternalTablePermissions(
            action: Action,
            columnAcls: List<Acl>,
            columnsById: Map<AclKey, TableColumn>
    ) {
        if (!actionTypeIsValid(action)) {
            return
        }

        updateTablePermissions(action, columnAcls, columnsById, TableType.TABLE)
    }

    private fun actionTypeIsValid(action: Action): Boolean {
        return when (action) {
            Action.ADD, Action.REMOVE, Action.SET -> {
                true
            }
            else -> {
                logger.error("Action $action passed through to updateTablePermissions is unhandled. Doing no operations")
                false
            }
        }
    }

    private fun getPrincipalToUsername(acls: List<Acl>): Map<Principal, String> {
        val principalAclKeys = principalsMapManager.getAclKeyByPrincipal(
                acls.flatMap { it.aces.map { a -> a.principal } }.toSet()
        )
        val usernamesByAclKey = dbCredentialService.getDbUsernamesAsMap(principalAclKeys.values.toSet())
        return principalAclKeys.mapValues { usernamesByAclKey.getValue(it.value) }
    }

    private fun updateTablePermissions(
            action: Action,
            columnAcls: List<Acl>,
            columnsById: Map<AclKey, TableColumn>,
            tableType: TableType,
            isAssembledEntitySet: Boolean = false
    ) {
        val principalToUsername = getPrincipalToUsername(columnAcls)

        val removes = mutableMapOf<UUID, MutableList<String>>()
        val adds = mutableMapOf<UUID, MutableList<String>>()

        columnAcls.groupBy { columnsById.getValue(it.aclKey).organizationId }.forEach { (orgId, orgColumnAcls) ->
            val orgAdds = mutableListOf<String>()
            val orgRemoves = mutableListOf<String>()

            orgColumnAcls.forEach { columnAcl ->
                val column = columnsById.getValue(columnAcl.aclKey)

                columnAcl.aces.forEach { ace ->
                    val userRole = principalToUsername.getValue(ace.principal)
                    val privileges = mapOLToPostgresPrivileges(tableType, ace.permissions)

                    when (action) {
                        Action.ADD -> {
                            orgAdds.addAll(grantPermissionsOnColumnSql(userRole, column, privileges, isAssembledEntitySet))
                        }
                        Action.REMOVE -> {
                            orgRemoves.addAll(revokePermissionsOnColumnSql(userRole, column, privileges))
                        }
                        Action.SET -> {
                            orgRemoves.addAll(revokePermissionsOnColumnSql(userRole, column, EnumSet.of(PostgresPrivileges.ALL)))
                            orgAdds.addAll(grantPermissionsOnColumnSql(userRole, column, privileges, isAssembledEntitySet))
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

    private fun mapOLToPostgresPrivileges(tableType: TableType, permissions: EnumSet<Permission>): Set<PostgresPrivileges> {
        val allPermissions = if (tableType == TableType.TABLE) allTablePermissions else allViewPermissions

        return permissions.filter { allPermissions.contains(it) }.flatMapTo(mutableSetOf()) { olToPostgres.getValue(it) }

    }

    private fun grantPermissionsOnColumnsOnTableToRoleSql(
            privileges: Set<PostgresPrivileges>,
            columns: String,
            schemaName: String,
            tableName: String,
            roleName: String
    ): String {
        val privilegeString = if (privileges.contains(PostgresPrivileges.ALL)) "ALL ( $columns )" else privileges.joinToString { privilege ->
            "$privilege ( $columns )"
        }
        return """
            GRANT $privilegeString 
            ON $schemaName.${ApiHelpers.dbQuote(tableName)}
            TO ${ApiHelpers.dbQuote(roleName)}
        """.trimIndent()
    }

    private fun revokePermissionsOnColumnsOnTableToRoleSql(
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
            REVOKE $privilegeString 
            ON $schemaName.${ApiHelpers.dbQuote(tableName)}
            FROM ${ApiHelpers.dbQuote(roleName)}
        """.trimIndent()
    }

    private fun grantPermissionsOnColumnSql(
            userRole: String,
            column: TableColumn,
            privileges: Set<PostgresPrivileges>,
            isAssembledEntitySet: Boolean
    ): List<String> {
        val sqls = mutableListOf(
                grantPermissionsOnColumnsOnTableToRoleSql(
                        privileges,
                        column.name,
                        column.schema.label,
                        column.tableName,
                        userRole
                ),
                grantUsageOnSchemaSql(column.schema, userRole)
        )
        if (isAssembledEntitySet) {
            sqls.add(
                    grantPermissionsOnColumnsOnTableToRoleSql(
                            privileges,
                            EdmConstants.ID_FQN.toString(),
                            column.schema.label,
                            column.tableName,
                            userRole
                    )
            )
        }

        return sqls
    }

    private fun revokePermissionsOnColumnSql(
            userRole: String,
            column: TableColumn,
            privileges: Set<PostgresPrivileges>
    ): List<String> {
        return listOf(
                revokePermissionsOnColumnsOnTableToRoleSql(
                        privileges,
                        column.name,
                        column.schema.label,
                        column.tableName,
                        userRole
                )
        )
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