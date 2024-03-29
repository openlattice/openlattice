package com.openlattice.postgres.external

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.edm.EntitySet
import com.openlattice.edm.PropertyTypeIdFqn
import com.openlattice.edm.processors.GetOrganizationIdFromEntitySetEntryProcessor
import com.openlattice.edm.processors.GetSchemaFromExternalTableEntryProcessor
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.IdConstants
import com.openlattice.organization.ExternalColumn
import com.openlattice.organization.ExternalTable
import com.openlattice.organization.roles.Role
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresPrivileges
import com.openlattice.postgres.TableColumn
import com.openlattice.postgres.external.Schemas
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

    private val securableObjectTypes = HazelcastMap.SECURABLE_OBJECT_TYPES.getMap(hazelcastInstance)
    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)
    private val propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap(hazelcastInstance)
    private val externalColumns = HazelcastMap.EXTERNAL_COLUMNS.getMap(hazelcastInstance)
    private val externalTables = HazelcastMap.EXTERNAL_TABLES.getMap(hazelcastInstance)

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
                statement.execute("REVOKE USAGE ON SCHEMA ${Schemas.PUBLIC_SCHEMA} FROM ${quote(dbRole)}")
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
                statement.execute("REVOKE USAGE ON SCHEMA ${Schemas.PUBLIC_SCHEMA} FROM ${quote(dbUser)}")
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
        val extTablesSchemaById = externalTables.submitToKeys(extTableIds, GetSchemaFromExternalTableEntryProcessor()).toCompletableFuture().get().mapValues {
            Schemas.fromName(it.value)
        }
        val columnsById = externalColumns.getAll(extTableColIds).values.associate {
            AclKey(it.tableId, it.id) to TableColumn(it.organizationId, it.tableId, it.id, extTablesSchemaById[it.tableId])
        }
        updateExternalTablePermissions(action, externalTableColAcls, columnsById)
    }

    fun executePrivilegesUpdateOnPropertyTypes(action: Action, assemblyAcls: List<Acl>) {
        val esids = assemblyAcls.aclKeysAsSet {
            it.aclKey[0]
        }
        val aclKeyToTableCols = entitySets.submitToKeys(esids, GetOrganizationIdFromEntitySetEntryProcessor()).thenApplyAsync { esidToOrgId ->
            assemblyAcls.associate {
                it.aclKey to TableColumn(esidToOrgId.getValue(it.aclKey[0]), it.aclKey[0], it.aclKey[1], Schemas.ASSEMBLED_ENTITY_SETS)
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

    fun toPostgres(aclKey: AclKey): String {
        return "'{" + aclKey.joinToString() + "}'::uuid[]"
    }

    /**
     * Updates permissions on [propertyTypes] for [entitySet] in org database for [organizationId]
     */
    override fun updateAssemblyPermissions(
            action: Action,
            columnAcls: List<Acl>,
            columnsById: Map<AclKey, TableColumn>
    ) {
        // add acl/TableColumn associated to EdmConstants.ID_FQN (with uuid IdConstants.ID_ID.id)
        val completedColumnAcls = columnAcls.toMutableList()
        val completedColumnsById = columnsById.toMutableMap()
        columnAcls.forEach {
            val internalIdAclKey = AclKey(it.aclKey[0], IdConstants.ID_ID.id)
            val orgId = columnsById.getValue(it.aclKey).organizationId
            val readAces = it.aces.map { ace ->
                Ace(ace.principal, EnumSet.of(Permission.READ))
            }

            completedColumnsById.put(
                internalIdAclKey,
                TableColumn(
                    orgId,
                    it.aclKey[0],
                    IdConstants.ID_ID.id,
                    Schemas.ASSEMBLED_ENTITY_SETS
                )
            )
            completedColumnAcls.add(Acl(internalIdAclKey, readAces))
        }

        logger.info("Permissioning for entity-sets in atlas currently disabled.")
        //updateTablePermissions(action, SecurableObjectType.PropertyTypeInEntitySet, completedColumnAcls, completedColumnsById, TableType.VIEW)
    }

    /**
     * Updates permissions on [columns] for [table] in org database for [organizationId]
     */
    override fun updateExternalTablePermissions(
            action: Action,
            columnAcls: List<Acl>,
            columnsById: Map<AclKey, TableColumn>
    ) {
        updateTablePermissions(action, SecurableObjectType.OrganizationExternalDatabaseColumn, columnAcls, columnsById, TableType.TABLE)
    }

    private fun updateTablePermissions(
            action: Action,
            securableObjectType: SecurableObjectType,
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
        when (securableObjectType) {
            SecurableObjectType.OrganizationExternalDatabaseColumn, SecurableObjectType.PropertyTypeInEntitySet -> {
            }
            else -> {
                logger.error("SecurableObjectType $securableObjectType passed through to updateTablePermissions is unhandled. Doing no operations")
                return
            }
        }

        val allPermissions = if (tableType == TableType.TABLE) allTablePermissions else allViewPermissions

        val principalAclKeys = principalsMapManager.getAclKeyByPrincipal(
                columnAcls.flatMap { it.aces.map { a -> a.principal } }.toSet()
        )
        val usernamesByAclKey = dbCredentialService.getDbUsernamesAsMap(principalAclKeys.values.toSet())
        val principalToUsername = principalAclKeys
            .filter { usernamesByAclKey.containsKey(it.value) }
            .mapValues { usernamesByAclKey.getValue(it.value) }

        val removes = mutableMapOf<UUID, MutableList<String>>()
        val adds = mutableMapOf<UUID, MutableList<String>>()

        columnAcls.groupBy { columnsById.getValue(it.aclKey).organizationId }.forEach { (orgId, orgColumnAcls) ->
            val orgAdds = mutableListOf<String>()
            val orgRemoves = mutableListOf<String>()

            orgColumnAcls.forEach { columnAcl ->
                val column = columnsById.getValue(columnAcl.aclKey)
                val columnName = if (securableObjectType == SecurableObjectType.OrganizationExternalDatabaseColumn) {
                    externalColumns.getValue(column.columnId)?.name ?: String()
                } else {
                    propertyTypes.getValue(column.columnId)?.type.toString() ?: String()
                }
                val tableName = if (securableObjectType == SecurableObjectType.OrganizationExternalDatabaseColumn) {
                    externalTables.getValue(column.tableId)?.name ?: String()
                } else {
                    entitySets.getValue(column.tableId)?.name ?: String()
                }
                val tableSchema = column.schema

                columnAcl.aces
                    .filter { principalToUsername.containsKey(it.principal) }
                    .forEach { ace ->
                        val userRole = principalToUsername.getValue(ace.principal)

                        // filter out permission which doesn't have a key in olToPostgres and not in allPermissions
                        val acePermissions = ace.permissions.filter {
                            olToPostgres.containsKey(it) && allPermissions.contains(it)
                        }
                        val requestedPermissions = acePermissions.flatMap { permission ->
                            olToPostgres.getValue(permission)
                        }

                        when (action) {
                            Action.ADD -> {
                                orgAdds.addAll(grantPermissionsOnColumnsOfTableSql(
                                        userRole,
                                        tableSchema,
                                        tableName,
                                        columnName,
                                        requestedPermissions
                                    ))
                            }
                            Action.REMOVE -> {
                                orgRemoves.addAll(removePermissionsOnColumnsOfTableSql(
                                        userRole,
                                        tableSchema,
                                        tableName,
                                        columnName,
                                        requestedPermissions
                                    ))
                            }
                            Action.SET -> {
                                val allColPermissions = allPermissions.flatMap { permission ->
                                    olToPostgres.getValue(permission)
                                }
                                orgRemoves.addAll(removePermissionsOnColumnsOfTableSql(
                                        userRole,
                                        tableSchema,
                                        tableName,
                                        columnName,
                                        allColPermissions
                                    ))

                                orgAdds.addAll(grantPermissionsOnColumnsOfTableSql(
                                        userRole,
                                        tableSchema,
                                        tableName,
                                        columnName,
                                        requestedPermissions
                                    ))
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
            logger.info("Updating permissions for org {}", organizationId)
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
                        logger.error("Exception occurred during external permissions update for org {}, rolling back", organizationId, ex)
                        conn.rollback()
                    }
                }
            }
        }
    }

    private fun removePermissionsOnColumnsOfTableSql(
            userRole: String,
            schema: Schemas?,
            tableName: String,
            columnName: String,
            privileges: List<PostgresPrivileges>
    ): List<String> {
        return privileges.map {
            applyPermissionOnColumnOfTableToRoleSql(
                    userRole,
                    schema,
                    tableName,
                    columnName,
                    it,
                    PgPermAction.REVOKE
            )
        }
    }

    private fun grantPermissionsOnColumnsOfTableSql(
            userRole: String,
            schema: Schemas?,
            tableName: String,
            columnName: String,
            privileges: List<PostgresPrivileges>
    ): List<String> {
        val sqls = mutableListOf<String>()

        privileges.forEach {
            sqls.add(
                applyPermissionOnColumnOfTableToRoleSql(
                    userRole,
                    schema,
                    tableName,
                    columnName,
                    it,
                    PgPermAction.GRANT
                )
            )

            if (schema != null) {
                sqls.add(grantUsageOnSchemaSql(schema, userRole))
            }
        }

        return sqls
    }

    private fun applyPermissionOnColumnOfTableToRoleSql(
            roleName: String,
            schema: Schemas?,
            tableName: String,
            columnName: String,
            privilege: PostgresPrivileges,
            action: PgPermAction
    ): String {

        val schemaName = if (schema != null) {
            schema.toString() + "."
        } else {
            ""
        }

        // there are column names with escaped characters, so not using string literals here
        return "${action.name} $privilege ( ${quote(columnName)} )\n" +
               "ON $schemaName${quote(tableName)}\n" +
               "${action.verb} ${quote(roleName)};"
    }

    private fun revokeRoleSql(roleName: String, targetRoles: Set<String>): String {
        return applyRoleOperation(roleName, targetRoles, PgPermAction.REVOKE)
    }

    private fun grantRoleToRole(roleName: String, targetRoles: Set<String>): String {
        return applyRoleOperation(roleName, targetRoles, PgPermAction.GRANT)
    }

    private fun applyRoleOperation(roleName: String, targetRoles: Set<String>, action: PgPermAction): String {
        val targets = targetRoles.joinToString { quote(it) }
        return "${action.name} ${quote(roleName)} ${action.verb} $targets"
    }

    internal fun createRoleIfNotExistsSql(dbRole: String): String {
        return """
            DO 
            ${'$'}do${'$'}
            BEGIN
                IF NOT EXISTS (
                    SELECT
                    FROM   pg_catalog.pg_roles
                    WHERE  rolname = '$dbRole') THEN
                    CREATE ROLE ${quote(dbRole)} NOCREATEDB NOCREATEROLE INHERIT NOLOGIN;
                END IF;
            END
            ${'$'}do${'$'};
        """.trimIndent()
    }

    internal fun createUserIfNotExistsSql(dbUser: String, dbUserPassword: String): String {
        return """
            DO 
            ${'$'}do${'$'}
            BEGIN
                IF NOT EXISTS (
                    SELECT
                    FROM   pg_catalog.pg_roles
                    WHERE  rolname = '$dbUser') THEN
                    CREATE ROLE ${quote(dbUser)} NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN ENCRYPTED PASSWORD '$dbUserPassword';
                END IF;
            END
            ${'$'}do${'$'};
        """.trimIndent()
    }

    private enum class PgPermAction(val verb: String, val quantifier: String) {
        GRANT("TO", "NOT"), REVOKE("FROM", "")
    }

    private enum class TableType {
        VIEW, TABLE
    }

}
