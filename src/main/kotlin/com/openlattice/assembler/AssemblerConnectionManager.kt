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
import com.google.common.collect.Sets
import com.google.common.eventbus.EventBus
import com.google.common.eventbus.Subscribe
import com.hazelcast.core.HazelcastInstance
import com.openlattice.assembler.PostgresDatabases.Companion.buildOrganizationDatabaseName
import com.openlattice.assembler.PostgresRoles.Companion.buildOrganizationUserId
import com.openlattice.assembler.PostgresRoles.Companion.buildPostgresRoleName
import com.openlattice.assembler.PostgresRoles.Companion.buildPostgresUsername
import com.openlattice.authorization.*
import com.openlattice.edm.EntitySet
import com.openlattice.edm.PostgresEdmTypeConverter
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.OrganizationEntitySetFlag
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.PostgresTable.*
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.openlattice.principals.RoleCreatedEvent
import com.openlattice.principals.UserCreatedEvent
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.sql.Connection
import java.sql.Statement
import java.util.*
import java.util.function.Function
import java.util.function.Supplier

private val logger = LoggerFactory.getLogger(AssemblerConnectionManager::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class AssemblerConnectionManager(
        private val assemblerConfiguration: AssemblerConfiguration,
        private val hds: HikariDataSource,
        private val securePrincipalsManager: SecurePrincipalsManager,
        private val organizations: HazelcastOrganizationService,
        private val dbCredentialService: DbCredentialService,
        hazelcastInstance: HazelcastInstance,
        eventBus: EventBus,
        metricRegistry: MetricRegistry
) {

    private val entitySets = hazelcastInstance.getMap<UUID, EntitySet>(HazelcastMap.ENTITY_SETS.name)
    private val target: HikariDataSource = connect("postgres")
    private val materializeAllTimer: Timer =
            metricRegistry.timer(name(AssemblerConnectionManager::class.java, "materializeAll"))
    private val materializeEntitySetsTimer: Timer =
            metricRegistry.timer(name(AssemblerConnectionManager::class.java, "materializeEntitySets"))
    private val materializeEdgesTimer: Timer =
            metricRegistry.timer(name(AssemblerConnectionManager::class.java, "materializeEdges"))

    init {
        eventBus.register(this)
    }

    fun connect(dbname: String): HikariDataSource {
        val config = assemblerConfiguration.server.clone() as Properties
        config.computeIfPresent("jdbcUrl") { _, jdbcUrl ->
            "${(jdbcUrl as String).removeSuffix(
                    "/"
            )}/$dbname" + if (assemblerConfiguration.ssl) {
                "?ssl=true"
            } else {
                ""
            }
        }
        return HikariDataSource(HikariConfig(config))
    }

    @Subscribe
    fun handleUserCreated(userCreatedEvent: UserCreatedEvent) {
        createUnprivilegedUser(userCreatedEvent.user)
    }

    @Subscribe
    fun handleRoleCreated(roleCreatedEvent: RoleCreatedEvent) {
        createRole(roleCreatedEvent.role)
    }

    /**
     * Creates a private organization database that can be used for uploading data using launchpad.
     * Also sets up foreign data wrapper using assembler in assembler so that materialized views of data can be
     * provided.
     */
    fun createOrganizationDatabase(organizationId: UUID) {
        val organization = organizations.getOrganization(organizationId)
        val dbname = buildOrganizationDatabaseName(organizationId)
        createOrganizationDatabase(organizationId, dbname)

        connect(dbname).use { datasource ->
            configureRolesInDatabase(datasource, securePrincipalsManager)
            createOpenlatticeSchema(datasource)

            datasource.connection.use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute(
                            "ALTER ROLE ${assemblerConfiguration.server["username"]} SET search_path to $PRODUCTION_FOREIGN_SCHEMA,$MATERIALIZED_VIEWS_SCHEMA,$PUBLIC_SCHEMA"
                    )
                }
            }

            addMembersToOrganization(dbname, datasource, organization.members)

            createForeignServer(datasource)
//                materializePropertyTypes(datasource)
//                materialzieEntityTypes(datasource)
        }
    }

    fun addMembersToOrganization(dbName: String, dataSource: HikariDataSource, members: Set<Principal>) {
        logger.info("Configuring members for organization database {}", dbName)
        members
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
                .forEach { principal ->
                    val securablePrincipal = securePrincipalsManager.getPrincipal(principal.id)
                    configureUserInDatabase(
                            dataSource,
                            dbName,
                            buildPostgresUsername(securablePrincipal)
                    )
                }
    }

    fun removeMembersFromOrganization(dbName: String, dataSource: HikariDataSource, members: Set<Principal>) {
        members.filter { it.id != SystemRole.OPENLATTICE.principal.id && it.id != SystemRole.ADMIN.principal.id }
                .filter {
                    securePrincipalsManager.principalExists(it)
                } //There are some bad principals in the member list some how-- probably from testing.
                .forEach { principal ->
                    revokeConnectAndSchemaUsage(
                            dataSource,
                            dbName,
                            buildPostgresUsername(securePrincipalsManager.getPrincipal(principal.id))
                    )
                }
    }

    private fun createOrganizationDatabase(organizationId: UUID, dbname: String) {
        val db = DataTables.quote(dbname)
        val dbRole = "${dbname}_role"
        val unquotedDbAdminUser = buildOrganizationUserId(organizationId)
        val dbOrgUser = DataTables.quote(unquotedDbAdminUser)
        val dbAdminUserPassword = dbCredentialService.getOrCreateUserCredentials(unquotedDbAdminUser)
                ?: dbCredentialService.getDbCredential(unquotedDbAdminUser)
        val createOrgDbRole = createRoleIfNotExistsSql(dbRole)
        val createOrgDbUser = createUserIfNotExistsSql(unquotedDbAdminUser, dbAdminUserPassword)

        val grantRole = "GRANT ${DataTables.quote(dbRole)} TO $dbOrgUser"
        val createDb = " CREATE DATABASE $db"
        val revokeAll = "REVOKE ALL ON DATABASE $db FROM $PUBLIC_SCHEMA"

        //We connect to default db in order to do initial db setup

        target.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(createOrgDbRole)
                statement.execute(createOrgDbUser)
                statement.execute(grantRole)
                if (!exists(dbname)) {
                    statement.execute(createDb)
                    //Allow usage of schema public
                    //statement.execute("REVOKE USAGE ON SCHEMA $PUBLIC_SCHEMA FROM ${DataTables.quote(dbOrgUser)}")
                    statement.execute("GRANT ${MEMBER_ORG_DATABASE_PERMISSIONS.joinToString(", ")} " +
                            "ON DATABASE $db TO $dbOrgUser")
                }
                statement.execute(revokeAll)
                return@use
            }
        }
    }

    fun dropOrganizationDatabase(organizationId: UUID) {
        dropOrganizationDatabase(organizationId, buildOrganizationDatabaseName(organizationId))
    }

    fun dropOrganizationDatabase(organizationId: UUID, dbname: String) {
        val db = quote(dbname)
        val dbRole = quote("${dbname}_role")
        val unquotedDbAdminUser = buildOrganizationUserId(organizationId)
        val dbAdminUser = quote(unquotedDbAdminUser)

        val dropDb = " DROP DATABASE $db"
        val dropDbUser = "DROP ROLE $dbAdminUser"
        //TODO: If we grant this role to other users, we need to make sure we drop it
        val dropDbRole = "DROP ROLE $dbRole"


        //We connect to default db in order to do initial db setup

        target.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(dropDb)
                statement.execute(dropDbUser)
                statement.execute(dropDbRole)
                return@use
            }
        }
    }

    /**
     * The reason we use an in the query for this function is that only entity sets that have been materialized
     * should have their edges materialized.
     */
    private fun materializeEdges(datasource: HikariDataSource, entitySetIds: Set<UUID>) {
        materializeEdgesTimer.time().use {
            val clause = entitySetIds.joinToString { entitySetId -> "'$entitySetId'" }
            datasource.connection.use { connection ->
                connection.createStatement().use { stmt ->
                    val tableName = "$MATERIALIZED_VIEWS_SCHEMA.${EDGES.name}"
                    stmt.execute("DROP MATERIALIZED VIEW IF EXISTS $tableName")
                    stmt.execute(
                            "CREATE MATERIALIZED VIEW IF NOT EXISTS $tableName AS " +
                                    "SELECT * FROM $PRODUCTION_FOREIGN_SCHEMA.${EDGES.name} " +
                                    "WHERE ${SRC_ENTITY_SET_ID.name} IN ($clause) " +
                                    "OR ${DST_ENTITY_SET_ID.name} IN ($clause) " +
                                    "OR ${EDGE_ENTITY_SET_ID.name} IN ($clause) "
                    )

                    val selectGrantedResults = grantSelectForEdges(stmt, tableName, entitySetIds)

                    logger.info("Granted select for ${selectGrantedResults.filter { it >= 0 }.size} users/roles " +
                            "on materialized view $tableName")
                    return@use
                }
            }
        }
    }

    fun materializeEntitySets(
            organizationId: UUID,
            authorizedPropertyTypesByEntitySet: Map<UUID, Map<UUID, PropertyType>>
    ): Map<UUID, Set<OrganizationEntitySetFlag>> {
        materializeAllTimer.time().use {
            connect(buildOrganizationDatabaseName(organizationId)).use { datasource ->
                materializeEntitySets(datasource, authorizedPropertyTypesByEntitySet)
            }
            return authorizedPropertyTypesByEntitySet.mapValues {
                EnumSet.of(
                        OrganizationEntitySetFlag.MATERIALIZED
                )
            }
        }
    }


    private fun materializeEntitySets(
            datasource: HikariDataSource,
            authorizedPropertyTypesByEntitySet: Map<UUID, Map<UUID, PropertyType>>
    ) {
        authorizedPropertyTypesByEntitySet.forEach { entitySetId, authorizedPropertyTypes ->
            createProductionForeignSchemaOfEntitySetIfNotExists(datasource, entitySetId)
            materialize(datasource, entitySetId, authorizedPropertyTypes)
        }

        materializeEdges(datasource, authorizedPropertyTypesByEntitySet.keys)
    }

    /**
     * Materializes an entity set on atlas.
     */
    private fun materialize(
            datasource: HikariDataSource, entitySetId: UUID,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ) {
        materializeEntitySetsTimer.time().use {
            val entitySet = entitySets.getValue(entitySetId)
            val propertyFqns = authorizedPropertyTypes
                    .mapValues { quote(it.value.type.fullQualifiedNameAsString) }
                    .values.joinToString(",")

            val sql = "SELECT ${ENTITY_SET_ID.name},${ID_VALUE.name}, $propertyFqns FROM $PRODUCTION_FOREIGN_SCHEMA.${quote(
                    entitySet.id.toString()
            )} "

            val tableName = "$MATERIALIZED_VIEWS_SCHEMA.${quote(entitySet.name)}"

            datasource.connection.use { connection ->
                val createMaterializedViewSql = "CREATE MATERIALIZED VIEW IF NOT EXISTS $tableName AS $sql"

                logger.info("Executing create materialize view sql: {}", createMaterializedViewSql)
                connection.createStatement().use { stmt ->
                    stmt.execute(createMaterializedViewSql)
                }
                //Next we need to grant select on materialize view to everyone who has permission.
                val selectGrantedResults = grantSelectForEntitySet(
                        connection,
                        tableName,
                        entitySet.id,
                        authorizedPropertyTypes
                )
                logger.info("Granted select for ${selectGrantedResults.filter { it >= 0 }.size} users/roles " +
                        "on materialized view $tableName")
            }
        }
    }

    private fun grantSelectForEntitySet(
            connection: Connection,
            tableName: String,
            entitySetId: UUID,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): IntArray {

        val permissions = EnumSet.of(Permission.READ)
        // collect all principals of type user, role, which have read access on entityset
        val authorizedPrincipals = securePrincipalsManager
                .getAuthorizedPrincipalsOnSecurableObject(AclKey(entitySetId), permissions)
                .filter { it.type == PrincipalType.USER || it.type == PrincipalType.ROLE }
                .toSet()
        // on every property type collect all principals of type user, role, which have read access
        val authorizedPrincipalsOfProperties = authorizedPropertyTypes.values.map {
            it.type to securePrincipalsManager
                    .getAuthorizedPrincipalsOnSecurableObject(AclKey(entitySetId, it.id), permissions)
                    .filter { it.type == PrincipalType.USER || it.type == PrincipalType.ROLE }
        }.toMap()

        // collect all authorized property types for principals which have read access on entity set
        val authorizedPropertiesOfPrincipal = authorizedPrincipals
                .map { it to mutableSetOf<FullQualifiedName>() }.toMap()
        authorizedPrincipalsOfProperties.forEach { fqn, principals ->
            principals.forEach {
                authorizedPropertiesOfPrincipal[it]?.add(fqn)
            }
        }

        // filter principals with no properties authorized
        authorizedPrincipalsOfProperties.filter { it.value.isEmpty() }

        // prepare batch queries
        return connection.createStatement().use { stmt ->
            authorizedPropertiesOfPrincipal.forEach { principal, fqns ->
                val allColumns =
                        if (fqns == authorizedPrincipalsOfProperties.keys) {
                            setOf() // if user is authorized for all properties, grant select on whole table
                        } else {
                            setOf(ENTITY_SET_ID.name, ID_VALUE.name) + fqns.map { it.fullQualifiedNameAsString }
                        }
                val grantSelectSql = grantSelectSql(tableName, principal, allColumns)
                stmt.addBatch(grantSelectSql)
            }
            stmt.executeBatch()
        }
    }

    fun grantSelectForEdges(stmt: Statement, tableName: String, entitySetIds: Set<UUID>): IntArray {
        val permissions = EnumSet.of(Permission.READ)
        // collect all principals of type user, role, which have read access on entityset
        val authorizedPrincipals = entitySetIds.fold(mutableSetOf<Principal>()) { acc, entitySetId ->
            Sets.union(acc,
                    securePrincipalsManager.getAuthorizedPrincipalsOnSecurableObject(AclKey(entitySetId), permissions)
                            .filter { it.type == PrincipalType.USER || it.type == PrincipalType.ROLE }
                            .toSet())
        }

        authorizedPrincipals.forEach {
            val grantSelectSql = grantSelectSql(tableName, it, setOf())
            stmt.addBatch(grantSelectSql)
        }

        return stmt.executeBatch()
    }

    /**
     * Build grant select sql statement for a given table and user with column level security.
     * If properties (columns) are left empty, it will grant select on whole table.
     */
    private fun grantSelectSql(
            entitySetTableName: String,
            principal: Principal,
            columns: Set<String>
    ): String {
        val postgresUserName = if (principal.type == PrincipalType.USER) {
            buildPostgresUsername(securePrincipalsManager.getPrincipal(principal.id))
        } else {
            buildPostgresRoleName(securePrincipalsManager.lookupRole(principal))
        }

        val onProperties = if (columns.isEmpty()) {
            ""
        } else {
            "(${columns.joinToString(",") { DataTables.quote(it) }}) "
        }

        return "GRANT SELECT $onProperties " +
                "ON $entitySetTableName " +
                "TO ${DataTables.quote(postgresUserName)}"
    }

    /**
     * Removes a materialized entity set from atlas.
     */
    fun dematerializeEntitySets(datasource: HikariDataSource, entitySetIds: Set<UUID>) {
        TODO("Drop the materialize view for the specified entity sets.")
    }

    internal fun exists(dbname: String): Boolean {
        target.connection.use { connection ->
            connection.createStatement().use { stmt ->
                stmt.executeQuery("select count(*) from pg_database where datname = '$dbname'").use { rs ->
                    rs.next()
                    return rs.getInt("count") > 0
                }
            }
        }
    }

    fun getAllRoles(spm: SecurePrincipalsManager): PostgresIterable<Role> {
        return PostgresIterable(
                Supplier {
                    val conn = hds.connection
                    val ps = conn.prepareStatement(PRINCIPALS_SQL)
                    ps.setString(1, PrincipalType.ROLE.name)
                    StatementHolder(conn, ps, ps.executeQuery())
                },
                Function { spm.getSecurablePrincipal(ResultSetAdapters.aclKey(it)) as Role }
        )
    }

    fun getAllUsers(spm: SecurePrincipalsManager): PostgresIterable<SecurablePrincipal> {
        return PostgresIterable(
                Supplier {
                    val conn = hds.connection
                    val ps = conn.prepareStatement(PRINCIPALS_SQL)
                    ps.setString(1, PrincipalType.USER.name)
                    StatementHolder(conn, ps, ps.executeQuery())
                },
                Function { spm.getSecurablePrincipal(ResultSetAdapters.aclKey(it)) }
        )
    }


    private fun configureRolesInDatabase(datasource: HikariDataSource, spm: SecurePrincipalsManager) {
        getAllRoles(spm).forEach { role ->
            val dbRole = DataTables.quote(buildPostgresRoleName(role))

            datasource.connection.use { connection ->
                connection.createStatement().use { statement ->
                    logger.info("Revoking $PUBLIC_SCHEMA schema right from role: {}", role)
                    //Don't allow users to access public schema which will contain foreign data wrapper tables.
                    statement.execute("REVOKE USAGE ON SCHEMA $PUBLIC_SCHEMA FROM $dbRole")

                    return@use
                }
            }
        }
    }

    fun dropUserIfExists(user: SecurablePrincipal) {
        target.connection.use { connection ->
            connection.createStatement().use { statement ->
                //TODO: Go through every database and for old users clean them out.
//                    logger.info("Attempting to drop owned by old name {}", user.name)
//                    statement.execute(dropOwnedIfExistsSql(user.name))
                logger.info("Attempting to drop user {}", user.name)
                statement.execute(dropUserIfExistsSql(user.name)) //Clean out the old users.
                dbCredentialService.deleteUserCredential(user.name)
                //Don't allow users to access public schema which will contain foreign data wrapper tables.
                logger.info("Revoking $PUBLIC_SCHEMA schema right from user {}", user)
            }
        }
    }

    fun createRole(role: Role) {
        val dbRole = buildPostgresRoleName(role)

        target.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(createRoleIfNotExistsSql(dbRole))
                //Don't allow users to access public schema which will contain foreign data wrapper tables.
                logger.info("Revoking $PUBLIC_SCHEMA schema right from role: {}", role)
                statement.execute("REVOKE USAGE ON SCHEMA $PUBLIC_SCHEMA FROM ${DataTables.quote(dbRole)}")

                return@use
            }
        }
    }

    fun createUnprivilegedUser(user: SecurablePrincipal) {
        val dbUser = buildPostgresUsername(user)
        //user.name
        val dbUserPassword = dbCredentialService.getOrCreateUserCredentials(dbUser)

        target.connection.use { connection ->
            connection.createStatement().use { statement ->
                //TODO: Go through every database and for old users clean them out.
//                    logger.info("Attempting to drop owned by old name {}", user.name)
//                    statement.execute(dropOwnedIfExistsSql(user.name))
//                    logger.info("Attempting to drop user {}", user.name)
//                    statement.execute(dropUserIfExistsSql(user.name)) //Clean out the old users.
//                    logger.info("Creating new user {}", dbUser)
                logger.info("Creating user if not exists {}", dbUser)
                statement.execute(createUserIfNotExistsSql(dbUser, dbUserPassword))
                //Don't allow users to access public schema which will contain foreign data wrapper tables.
                logger.info("Revoking $PUBLIC_SCHEMA schema right from user {}", user)
                statement.execute("REVOKE USAGE ON SCHEMA $PUBLIC_SCHEMA FROM ${DataTables.quote(dbUser)}")

                return@use
            }
        }
    }

    private fun configureUserInDatabase(datasource: HikariDataSource, dbname: String, userId: String) {
        val dbUser = DataTables.quote(userId)
        logger.info("Configuring user {} in database {}", userId, dbname)
        //First we will grant all privilege which for database is connect, temporary, and create schema
        target.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("GRANT ${MEMBER_ORG_DATABASE_PERMISSIONS.joinToString(", ")} " +
                        "ON DATABASE ${quote(dbname)} TO $dbUser")
            }
        }

        datasource.connection.use { connection ->
            connection.createStatement().use { statement ->

                statement.execute("GRANT USAGE ON SCHEMA $MATERIALIZED_VIEWS_SCHEMA TO $dbUser")
                //Don't allow users to access public schema which will contain foreign data wrapper tables.
                logger.info("Revoking $PUBLIC_SCHEMA schema right from user: {}", userId)
                statement.execute("REVOKE USAGE ON SCHEMA $PUBLIC_SCHEMA FROM $dbUser")
                //Set the search path for the user
                statement.execute("ALTER USER $dbUser set search_path TO $MATERIALIZED_VIEWS_SCHEMA")

                return@use
            }
        }
    }

    private fun revokeConnectAndSchemaUsage(datasource: HikariDataSource, dbname: String, userId: String) {
        val dbUser = DataTables.quote(userId)
        logger.info("Removing user {} from database {} and schema usage of $MATERIALIZED_VIEWS_SCHEMA", userId, dbname)

        datasource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("REVOKE ${MEMBER_ORG_DATABASE_PERMISSIONS.joinToString(", ")} " +
                        "ON DATABASE ${quote(dbname)} FROM $dbUser")
                stmt.execute("REVOKE ALL PRIVILEGES ON SCHEMA $MATERIALIZED_VIEWS_SCHEMA FROM $dbUser")
            }
        }
    }

    private fun createForeignServer(datasource: HikariDataSource) {
        logger.info("Setting up foreign server for datasource: {}", datasource.jdbcUrl)
        datasource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw")

                logger.info("Installed postgres_fdw extension.")

                statement.execute(
                        "CREATE SERVER IF NOT EXISTS $PRODUCTION_SERVER FOREIGN DATA WRAPPER postgres_fdw " +
                                "OPTIONS (host '${assemblerConfiguration.foreignHost}', " +
                                "dbname '${assemblerConfiguration.foreignDbName}', " +
                                "port '${assemblerConfiguration.foreignPort}')"
                )
                logger.info("Created foreign server definition. ")
                statement.execute(
                        "CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER $PRODUCTION_SERVER " +
                                "OPTIONS ( user '${assemblerConfiguration.foreignUsername}', " +
                                "password '${assemblerConfiguration.foreignPassword}')"
                )
                logger.info("Reseting $PRODUCTION_FOREIGN_SCHEMA")
                statement.execute("DROP SCHEMA IF EXISTS $PRODUCTION_FOREIGN_SCHEMA CASCADE")
                statement.execute("CREATE SCHEMA IF NOT EXISTS $PRODUCTION_FOREIGN_SCHEMA")
                logger.info("Created user mapping. ")
                statement.execute(importProductionViewsSchemaSql(setOf()))
                statement.execute(
                        importPublicSchemaSql(setOf(EDGES.name, PROPERTY_TYPES.name, ENTITY_TYPES.name, ENTITY_SETS.name))
                )
                logger.info("Imported foreign schema")
            }
        }
    }

    fun createProductionForeignSchemaOfEntitySetIfNotExists(organizationId: UUID, entitySetId: UUID) {
        val dbname = PostgresDatabases.buildOrganizationDatabaseName(organizationId)
        connect(dbname).use { datasource ->
            createProductionForeignSchemaOfEntitySetIfNotExists(datasource, entitySetId)
        }
    }

    private fun createProductionForeignSchemaOfEntitySetIfNotExists(dataSource: HikariDataSource, entitySetId: UUID) {
        dataSource.connection.use { conn ->
            // check if table exists and import if not
            if (!checkIfProductionViewsSchemaExists(conn, entitySetId)) {
                conn.createStatement().use { stmt ->
                    stmt.execute(importProductionViewsSchemaSql(setOf(DataTables.quote(entitySetId.toString()))))
                    // (re)-import entity_sets
                    updatePublicTables(stmt, setOf(PostgresTable.ENTITY_SETS.name))
                }
            }
        }
    }

    private fun checkIfProductionViewsSchemaExists(connection: Connection, entitySetId: UUID): Boolean {
        val stmt = connection
                .prepareStatement("SELECT EXISTS " +
                        "(SELECT 1 FROM ( SELECT to_regclass(?) AS name) AS foo WHERE foo.name IS NOT NULL)")
        stmt.setObject(1, "$PRODUCTION_FOREIGN_SCHEMA.${DataTables.quote(entitySetId.toString())}")

        val rs = stmt.executeQuery()
        rs.next()
        return ResultSetAdapters.exists(rs)
    }

    fun updateProductionForeignSchemaOfEntitySet(
            organizationId: UUID, entitySetId: UUID, newPropertyTypes: List<PropertyType>) {
        val dbname = PostgresDatabases.buildOrganizationDatabaseName(organizationId)
        connect(dbname).use { datasource ->
            datasource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    // add new columns
                    newPropertyTypes.forEach {
                        val newColumn = PostgresColumnDefinition(
                                quote(it.type.fullQualifiedNameAsString),
                                PostgresEdmTypeConverter.map(it.datatype))
                        stmt.execute("ALTER FOREIGN TABLE $PRODUCTION_FOREIGN_SCHEMA.\"$entitySetId\" ADD COLUMN ${newColumn.sql()}")
                    }

                    // (re)-import entity_sets, property_types, entity_types
                    val publicTables = mutableSetOf(
                            PostgresTable.ENTITY_SETS.name,
                            PostgresTable.PROPERTY_TYPES.name,
                            PostgresTable.ENTITY_TYPES.name)
                    updatePublicTables(stmt, publicTables)
                }
            }
        }
    }

    private fun updatePublicTables(stmt: Statement, tables: Set<String>) {
        tables.forEach {
            stmt.execute("DROP FOREIGN TABLE IF EXISTS $PRODUCTION_FOREIGN_SCHEMA.$it CASCADE")
        }
        stmt.execute(importPublicSchemaSql(tables))
    }

}

const val MATERIALIZED_VIEWS_SCHEMA = "openlattice"
const val PRODUCTION_FOREIGN_SCHEMA = "prod"
const val PRODUCTION_VIEWS_SCHEMA = "olviews"  //This is the scheme that is created on production server to hold entity set views
const val PUBLIC_SCHEMA = "public"
const val PRODUCTION_SERVER = "olprod"

val MEMBER_ORG_DATABASE_PERMISSIONS = setOf("CREATE", "CONNECT", "TEMPORARY", "TEMP")


private val PRINCIPALS_SQL = "SELECT acl_key FROM principals WHERE ${PRINCIPAL_TYPE.name} = ?"

internal fun createSchema(datasource: HikariDataSource, schema: String) {
    datasource.connection.use { connection ->
        connection.createStatement().use { statement ->
            statement.execute("CREATE SCHEMA IF NOT EXISTS $schema")
        }
    }
}

internal fun createOpenlatticeSchema(datasource: HikariDataSource) {
    createSchema(datasource, SCHEMA)
}

internal fun importProductionViewsSchemaSql(limitTo: Set<String>): String {
    return importForeignSchema(PRODUCTION_VIEWS_SCHEMA, limitTo)
}

internal fun importPublicSchemaSql(limitTo: Set<String>): String {
    return importForeignSchema(PUBLIC_SCHEMA, limitTo)
}

internal fun importForeignSchema(from: String, limitTo: Set<String>): String {
    val limitToSql = if(limitTo.isEmpty()) "" else "LIMIT TO ( ${limitTo.joinToString(", ")} )"
    return "IMPORT FOREIGN SCHEMA $from $limitToSql FROM SERVER $PRODUCTION_SERVER INTO $PRODUCTION_FOREIGN_SCHEMA"
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
            "      CREATE ROLE ${DataTables.quote(
                    dbRole
            )} NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOLOGIN;\n" +
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
            "      CREATE ROLE ${DataTables.quote(
                    dbUser
            )} NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN ENCRYPTED PASSWORD '$dbUserPassword';\n" +
            "   END IF;\n" +
            "END\n" +
            "\$do\$;"
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
            "      DROP OWNED BY ${DataTables.quote(
                    dbUser
            )} ;\n" +
            "   END IF;\n" +
            "END\n" +
            "\$do\$;"
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
            "      DROP ROLE ${DataTables.quote(
                    dbUser
            )} ;\n" +
            "   END IF;\n" +
            "END\n" +
            "\$do\$;"
}



