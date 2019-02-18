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
import com.hazelcast.core.IMap
import com.openlattice.assembler.PostgresRoles.Companion.buildOrganizationUserId
import com.openlattice.assembler.PostgresRoles.Companion.buildPostgresRoleName
import com.openlattice.assembler.PostgresRoles.Companion.buildPostgresUsername
import com.openlattice.authorization.*
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.organization.OrganizationEntitySetFlag
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.util.*
import java.util.function.Function
import java.util.function.Supplier


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class AssemblerConnectionManager {
    companion object {
        private val logger = LoggerFactory.getLogger(AssemblerConnectionManager::class.java)

        lateinit var assemblerConfiguration: AssemblerConfiguration

        private lateinit var hds: HikariDataSource
        private lateinit var securePrincipalsManager: SecurePrincipalsManager
        private lateinit var organizations: HazelcastOrganizationService
        private lateinit var dbCredentialService: DbCredentialService
        private lateinit var entitySets: IMap<UUID, EntitySet>
        private lateinit var target: HikariDataSource
        private lateinit var materializeAllTimer: Timer
        private lateinit var materializeEntitySetsTimer: Timer
        private lateinit var materializeEdgesTimer: Timer


        private val initialized = BooleanArray(7) { false }

        @JvmStatic
        fun initializeEntitySets(entitySets: IMap<UUID, EntitySet>) {
            if (initialized[0]) {
                logger.info("Ignoring entity sets in assembler as it is already initialized.")
            } else {
                this.entitySets = entitySets
                initialized[0] = true
                logger.info("Initialized entity sets in assembler")
            }
        }


        @JvmStatic
        fun initializeProductionDatasource(hds: HikariDataSource) {
            if (initialized[1]) {
                logger.info("Ignoring production datasource in assembler as it is already initialized.")
            } else {
                this.hds = hds
                hds.connection.use { conn ->
                    conn.createStatement().use { stmt ->
                        stmt.execute("CREATE SCHEMA IF NOT EXISTS $PRODUCTION_VIEWS_SCHEMA")
                    }
                    logger.info("Verified $PRODUCTION_VIEWS_SCHEMA schema exists.")
                }

                logger.info("Initialized production datasource in assembler.")
                initialized[1] = true
                initializeUsersAndRoles()
            }
        }

        @JvmStatic
        fun initializeOrganizations(organizations: HazelcastOrganizationService) {
            if (initialized[2]) {
                logger.info("Ignoring organizations in assembler as it is already initialized.")
            } else {
                this.organizations = organizations
                initialized[2] = true
                logger.info("Initialized organizations in assembler.")
            }
        }

        @JvmStatic
        fun initializeAssemblerConfiguration(assemblerConfiguration: AssemblerConfiguration) {
            if (initialized[3]) {
                logger.info("Ignoring assembler in assembler {} as it is already initialized.", assemblerConfiguration)
            } else {
                this.assemblerConfiguration = assemblerConfiguration
                target = connect("postgres")
                initialized[3] = true
                logger.info("Assembler in assembler initialized to: {}", assemblerConfiguration)
            }
        }

        @JvmStatic
        fun initializeSecurePrincipalsManager(securePrincipalsManager: SecurePrincipalsManager) {
            if (initialized[4]) {
                logger.info("Ignoring principals manager as it is already initialized.")
            } else {
                this.securePrincipalsManager = securePrincipalsManager
                logger.info("Principals manager initialized.")
                initialized[4] = true
                initializeUsersAndRoles()
            }
        }

        @JvmStatic
        fun initializeUsersAndRoles() {
            if (initialized[4] && initialized[0] && initialized[5]) {
                getAllRoles(securePrincipalsManager).map(::createRole)
                getAllUsers(securePrincipalsManager).map(::createUnprivilegedUser)
                logger.info("Creating users and roles.")
            }

            if (initialized[3]) {
                target = connect("postgres")
            }
        }

        @JvmStatic
        fun initializeDbCredentialService(dbCredentialService: DbCredentialService) {
            if (initialized[5]) {
                logger.info("Ignoring db credential service as it is already initialized.", dbCredentialService)
            } else {
                this.dbCredentialService = dbCredentialService
                logger.info("Db credential service initialized.")
                initialized[5] = true
                initializeUsersAndRoles()
            }
        }

        @JvmStatic
        fun initializeMetrics(metricRegistry: MetricRegistry) {
            if (initialized[6]) {
                logger.info("Ignoring metrics registry as it is already initialized.")
            } else {
                this.materializeAllTimer = metricRegistry.timer(
                        name(AssemblerConnectionManager::class.java, "materializeAll")
                )
                this.materializeEntitySetsTimer = metricRegistry.timer(
                        name(AssemblerConnectionManager::class.java, "materializeEntitySets")
                )
                this.materializeEdgesTimer = metricRegistry.timer(
                        name(AssemblerConnectionManager::class.java, "materializeEdges")
                )
                logger.info("Principals manager initialized.")
                initialized[6] = true
                initializeUsersAndRoles()
            }
        }

        @JvmStatic
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


        /**
         * Creates a private organization database that can be used for uploading data using launchpad.
         * Also sets up foreign data wrapper using assembler in assembler so that materialized views of data can be
         * provided.
         */
        @JvmStatic
        fun createOrganizationDatabase(organizationId: UUID) {
            val organization = organizations.getOrganization(organizationId)
            createOrganizationDatabase(organizationId, organization.principal.id)

            connect(organization.principal.id).use { datasource ->
                configureRolesInDatabase(datasource, securePrincipalsManager)
                createOpenlatticeSchema(datasource)

                datasource.connection.use { connection ->
                    connection.createStatement().use { statement ->
                        statement.execute(
                                "ALTER ROLE ${assemblerConfiguration.server["username"]} SET search_path to $PRODUCTION_FOREIGN_SCHEMA,$SCHEMA,public"
                        )
                    }
                }

                organization.members
                        .filter { it.id != "openlatticeRole" && it.id != "admin" }
                        .forEach { principal ->
                            configureUserInDatabase(
                                    datasource,
                                    organization.principal.id,
                                    buildPostgresUsername(securePrincipalsManager.getPrincipal(principal.id))
                            )
                        }

                createForeignServer(datasource)
//                materializePropertyTypes(datasource)
//                materialzieEntityTypes(datasource)
            }
        }

        @JvmStatic
        internal fun createOrganizationDatabase(organizationId: UUID, dbname: String) {
            val dbCredentialService = AssemblerConnectionManager.dbCredentialService
            val db = DataTables.quote(dbname)
            val dbRole = "${dbname}_role"
            val unquotedDbAdminUser = buildOrganizationUserId(organizationId)
            val dbOrgUser = DataTables.quote(unquotedDbAdminUser)
            val dbAdminUserPassword = dbCredentialService.createUserIfNotExists(unquotedDbAdminUser)
                    ?: dbCredentialService.getDbCredential(unquotedDbAdminUser)
            val createOrgDbRole = createRoleIfNotExistsSql(dbRole)
            val createOrgDbUser = createUserIfNotExistsSql(unquotedDbAdminUser, dbAdminUserPassword)

            val grantRole = "GRANT ${DataTables.quote(dbRole)} TO $dbOrgUser"
            val createDb = " CREATE DATABASE $db"
            val revokeAll = "REVOKE ALL ON DATABASE $db FROM public"

            //We connect to default db in order to do initial db setup

            target.connection.use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute(createOrgDbRole)
                    statement.execute(createOrgDbUser)
                    statement.execute(grantRole)
                    if (!exists(dbname)) {
                        statement.execute(createDb)
                        //Allow usage of schema public
                        //statement.execute("REVOKE USAGE ON SCHEMA public FROM ${DataTables.quote(dbOrgUser)}")
                        statement.execute("GRANT ALL PRIVILEGES ON DATABASE $db TO $dbOrgUser")
                    }
                    statement.execute(revokeAll)
                    return@use
                }
            }
        }

        private fun dropDatabase(organizationId: UUID, dbname: String) {
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
                        stmt.execute("DROP MATERIALIZED VIEW IF EXISTS $SCHEMA.edges")
                        stmt.execute(
                                "CREATE MATERIALIZED VIEW IF NOT EXISTS $SCHEMA.edges AS SELECT * FROM $PRODUCTION_FOREIGN_SCHEMA.edges WHERE src_entity_set_id IN ($clause) " +
                                        "AND dst_entity_set_id IN ($clause) " +
                                        "AND edge_entity_set_id IN ($clause) "
                        )
                        return@use
                    }
                }
            }
        }

        @JvmStatic
        fun materializeEntitySets(
                organizationId: UUID,
                authorizedPropertyTypesByEntitySet: Map<UUID, Map<UUID, PropertyType>>
        ): Map<UUID, Set<OrganizationEntitySetFlag>> {
            materializeAllTimer.time().use {
                connect(organizations.getOrganizationPrincipal(organizationId).name).use { datasource ->
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
                val entitySet = entitySets[entitySetId]!!
                val propertyFqns = authorizedPropertyTypes
                        .mapValues { quote(it.value.type.fullQualifiedNameAsString) }
                        .values.joinToString(",")

                val sql = "SELECT ${ENTITY_SET_ID.name},${ID_VALUE.name}, $propertyFqns FROM $PRODUCTION_FOREIGN_SCHEMA.${quote(
                        entitySet.id.toString()
                )} "

                val tableName = "$MATERIALIZED_VIEWS_SCHEMA.${quote(entitySet.name)}"

                datasource.connection.use { connection ->
                    val sql = "CREATE MATERIALIZED VIEW IF NOT EXISTS $tableName AS $sql"

                    logger.info("Executing create materialize view sql: {}", sql)
                    connection.createStatement().use { stmt ->
                        stmt.execute(sql)
                    }
                    //Next we need to grant select on materialize view to everyone who has permission.
                    val selectGrantedCount = grantSelectForEntitySet(
                            connection, tableName, entitySet.id, authorizedPropertyTypes
                    )
                    logger.info(
                            "Granted select for $selectGrantedCount users/roles on materialized view " +
                                    "$MATERIALIZED_VIEWS_SCHEMA.${quote(entitySet.name)}"
                    )
                }
            }
        }

        private fun grantSelectForEntitySet(
                connection: Connection,
                tableName: String,
                entitySetId: UUID,
                authorizedPropertyTypes: Map<UUID, PropertyType>
        ): Int {

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
                    val grantSelectSql = grantSelectSql(tableName, principal, fqns)
                    stmt.addBatch(grantSelectSql)
                }
                stmt.executeBatch()
            }.sum()
        }

        private fun grantSelectSql(
                entitySetTableName: String,
                principal: Principal,
                properties: Set<FullQualifiedName>
        ): String {
            val postgresUserName = if (principal.type == PrincipalType.USER) {
                DataTables.quote(principal.id)
            } else {
                buildPostgresRoleName(securePrincipalsManager.lookupRole(principal))
            }
            return "GRANT SELECT " +
                    "(${properties.joinToString(",") { DataTables.quote(it.fullQualifiedNameAsString) }}) " +
                    "ON $entitySetTableName " +
                    "TO $postgresUserName"
        }

        /**
         * Removes a materialized entity set from atlas.
         */
        @JvmStatic
        fun dematerializeEntitySets(datasource: HikariDataSource, entitySetIds: Set<UUID>) {
            TODO("Drop the materialize view for the specified entity sets.")
        }

        @JvmStatic
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

        private fun getAllRoles(spm: SecurePrincipalsManager): PostgresIterable<Role> {
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

        private fun getAllUsers(spm: SecurePrincipalsManager): PostgresIterable<SecurablePrincipal> {
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
                        logger.info("Revoking public schema right from role: {}", role)
                        //Don't allow users to access public schema which will contain foreign data wrapper tables.
                        statement.execute("REVOKE USAGE ON SCHEMA public FROM $dbRole")

                        return@use
                    }
                }
            }
        }


        @JvmStatic
        fun createRole(role: Role) {
            val dbRole = buildPostgresRoleName(role)

            target.connection.use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute(createRoleIfNotExistsSql(dbRole))
                    //Don't allow users to access public schema which will contain foreign data wrapper tables.
                    logger.info("Revoking public schema right from role: {}", role)
                    statement.execute("REVOKE USAGE ON SCHEMA public FROM ${DataTables.quote(dbRole)}")

                    return@use
                }
            }
        }

        @JvmStatic
        fun createUnprivilegedUser(user: SecurablePrincipal) {
            val dbUser = buildPostgresUsername(user)
            //user.name
            val dbUserPassword = dbCredentialService.getDbCredential(user.name)

            target.connection.use { connection ->
                connection.createStatement().use { statement ->
                    //TODO: Go through every database and for old users clean them out.
//                    logger.info("Attempting to drop owned by old name {}", user.name)
//                    statement.execute(dropOwnedIfExistsSql(user.name))
//                    logger.info("Attempting to drop user {}", user.name)
//                    statement.execute(dropUserIfExistsSql(user.name)) //Clean out the old users.
//                    logger.info("Creating new user {}", dbUser)
                    statement.execute(createUserIfNotExistsSql(dbUser, dbUserPassword))
                    //Don't allow users to access public schema which will contain foreign data wrapper tables.
                    logger.info("Revoking public schema right from user {}", user)
                    statement.execute("REVOKE USAGE ON SCHEMA public FROM ${DataTables.quote(dbUser)}")

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
                    statement.execute("GRANT ALL PRIVILEGES ON DATABASE ${quote(dbname)} TO $dbUser")
                }
            }

            datasource.connection.use { connection ->
                connection.createStatement().use { statement ->

                    statement.execute("GRANT USAGE ON SCHEMA $SCHEMA TO $dbUser")
                    //Don't allow users to access public schema which will contain foreign data wrapper tables.
                    logger.info("Revoking public schema right from user: {}", userId)
                    statement.execute("REVOKE USAGE ON SCHEMA public FROM $dbUser")
                    //Set the search path for the user
                    statement.execute("ALTER USER $dbUser set search_path TO $SCHEMA,public")

                    return@use
                }
            }
        }

        @JvmStatic
        internal fun createForeignServer(datasource: HikariDataSource) {
            logger.info("Setting up foreign server for datasource: {}", datasource.jdbcUrl)
            datasource.connection.use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw")

                    logger.info("Installed postgres_fdw extension.")

                    statement.execute(
                            "CREATE SERVER IF NOT EXISTS $PRODUCTION FOREIGN DATA WRAPPER postgres_fdw " +
                                    "OPTIONS (host '${AssemblerConnectionManager.assemblerConfiguration.foreignHost}', " +
                                    "dbname '${AssemblerConnectionManager.assemblerConfiguration.foreignDbName}', " +
                                    "port '${AssemblerConnectionManager.assemblerConfiguration.foreignPort}')"
                    )
                    logger.info("Created foreign server definition. ")
                    statement.execute(
                            "CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER $PRODUCTION " +
                                    "OPTIONS ( user '${AssemblerConnectionManager.assemblerConfiguration.foreignUsername}', " +
                                    "password '${AssemblerConnectionManager.assemblerConfiguration.foreignPassword}')"
                    )
                    logger.info("Reseting $PRODUCTION_FOREIGN_SCHEMA")
                    statement.execute("DROP SCHEMA IF EXISTS $PRODUCTION_FOREIGN_SCHEMA CASCADE")
                    statement.execute("CREATE SCHEMA IF NOT EXISTS $PRODUCTION_FOREIGN_SCHEMA")
                    logger.info("Created user mapping. ")
                    statement.execute(
                            "IMPORT FOREIGN SCHEMA $PRODUCTION_VIEWS_SCHEMA FROM SERVER $PRODUCTION INTO $PRODUCTION_FOREIGN_SCHEMA"
                    )
                    statement.execute(
                            "IMPORT FOREIGN SCHEMA public LIMIT TO (edges, property_types, entity_types, entity_sets) FROM SERVER $PRODUCTION INTO $PRODUCTION_FOREIGN_SCHEMA"
                    )
                    logger.info("Imported foreign schema")
                }
            }
        }

    }
}

const val MATERIALIZED_VIEWS_SCHEMA = "openlattice"
const val PRODUCTION_FOREIGN_SCHEMA = "prod"
const val PRODUCTION_VIEWS_SCHEMA = "olviews"

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



