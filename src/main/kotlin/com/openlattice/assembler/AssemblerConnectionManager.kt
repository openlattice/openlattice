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

import com.hazelcast.core.IMap
import com.openlattice.authorization.DbCredentialService
import com.openlattice.authorization.PrincipalType
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.selectEntitySetWithCurrentVersionOfPropertyTypes
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import organization.OrganizationEntitySetFlag
import java.util.*
import java.util.function.Function
import java.util.function.Supplier

private val logger = LoggerFactory.getLogger(AssemblerConnectionManager::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class AssemblerConnectionManager {
    companion object {
        private val logger = LoggerFactory.getLogger(AssemblerConnectionManager::class.java)
        private lateinit var assemblerConfiguration: AssemblerConfiguration
        private lateinit var hds: HikariDataSource
        private lateinit var securePrincipalsManager: SecurePrincipalsManager
        private lateinit var organizations: HazelcastOrganizationService
        private lateinit var dbCredentialService: DbCredentialService
        private lateinit var entitySets: IMap<UUID, EntitySet>
        private lateinit var target: HikariDataSource


        @JvmStatic
        fun initializeEntitySets(entitySets: IMap<UUID, EntitySet>) {
            if (this::entitySets.isInitialized) {
                logger.info("Ignoring entity sets in assembler as it is already initialized.")
            } else {
                this.entitySets = entitySets
                logger.info("Initialized entity sets in assembler")
            }
        }


        @JvmStatic
        fun initializeProductionDatasource(hds: HikariDataSource) {
            if (this::hds.isInitialized) {
                logger.info("Ignoring production datasource in assembler as it is already initialized.")
            } else {
                this.hds = hds
                logger.info("Initialized production datasource in assembler")
                initializeUsersAndRoles()
            }
        }

        @JvmStatic
        fun initializeOrganizatons(organizations: HazelcastOrganizationService) {
            if (this::organizations.isInitialized) {
                logger.info("Ignoring organizations in assembler as it is already initialized.")
            } else {
                this.organizations = organizations

                logger.info("Initialized organizations in assembler.")
            }
        }

        @JvmStatic
        fun initializeAssemblerConfiguration(assemblerConfiguration: AssemblerConfiguration) {
            if (this::assemblerConfiguration.isInitialized) {
                logger.info("Ignoring assembler in assembler {} as it is already initialized.", assemblerConfiguration)
            } else {
                this.assemblerConfiguration = assemblerConfiguration
                target = connect("postgres")
                logger.info("Assembler in assembler initialized to: {}", assemblerConfiguration)
            }
        }

        @JvmStatic
        fun initializeSecurePrincipalsManager(securePrincipalsManager: SecurePrincipalsManager) {
            if (this::securePrincipalsManager.isInitialized) {
                logger.info("Ignoring principals manager as it is already initialized.")
            } else {
                this.securePrincipalsManager = securePrincipalsManager
                logger.info("Principals manager initialized.")
                initializeUsersAndRoles()
            }
        }

        private fun initializeUsersAndRoles() {
            if( this::securePrincipalsManager.isInitialized && this::hds.isInitialized ) {
                getAllRoles(securePrincipalsManager).map(::createRole)
                getAllUsers(securePrincipalsManager).map(::createUnprivilegedUser)
                logger.info("Creating users and roles.")
            }
        }

        @JvmStatic
        fun initializeDbCredentialService(dbCredentialService: DbCredentialService) {
            if (this::dbCredentialService.isInitialized) {
                logger.info("Ignoring db credential service as it is already initialized.", dbCredentialService)
            } else {
                this.dbCredentialService = dbCredentialService
                logger.info("Db credential service initialized.")
            }
        }

        @JvmStatic
        fun getSecurePrincipalsManager(): SecurePrincipalsManager {
            return securePrincipalsManager
        }

        @JvmStatic
        fun getDbCredentialService(): DbCredentialService {
            return dbCredentialService
        }

        @JvmStatic
        fun connect(dbname: String): HikariDataSource {
            val config = assemblerConfiguration.server.clone() as Properties
            config.computeIfPresent("jdbcUrl") { _, jdbcUrl ->
                "${(jdbcUrl as String).removeSuffix(
                        "/"
                )}/$dbname?ssl=true"
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
            createDatabase(organizationId, organization.principal.id)

            connect(organization.principal.id).use { datasource ->
                configureRolesInDatabase(datasource, securePrincipalsManager)
                createOpenlatticeSchema(datasource)

                organization.members
                        .filter { it.id != "openlatticeRole" && it.id != "admin" }
                        .forEach { principal -> configureUserInDatabase(datasource, principal.id) }

                createForeignServer(datasource)
//                materializePropertyTypes(datasource)
//                materialzieEntityTypes(datasource)
            }
        }

        @JvmStatic
        internal fun createDatabase(organizationId: UUID, dbname: String) {
            val dbCredentialService = AssemblerConnectionManager.getDbCredentialService()
            val db = DataTables.quote(dbname)
            val dbRole = "${dbname}_role"
            val unquotedDbAdminUser = buildUserId(organizationId)
            val dbAdminUser = DataTables.quote(unquotedDbAdminUser)
            val dbAdminUserPassword = dbCredentialService.createUserIfNotExists(unquotedDbAdminUser)
                    ?: dbCredentialService.getDbCredential(unquotedDbAdminUser)
            val createDbRole = createRoleIfNotExistsSql(dbRole)
            val createDbUser = createUserIfNotExistsSql(unquotedDbAdminUser, dbAdminUserPassword)
            val grantRole = "GRANT ${DataTables.quote(dbRole)} TO $dbAdminUser"
            val createDb = " CREATE DATABASE $db WITH OWNER=$dbAdminUser"
            val revokeAll = "REVOKE ALL ON DATABASE $db FROM public"

            //We connect to default db in order to do initial db setup

            target.connection.use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute(createDbRole)
                    statement.execute(createDbUser)
                    statement.execute(grantRole)
                    if (!exists(dbname)) {
                        statement.execute(createDb)
                    }
                    statement.execute(revokeAll)
                    return@use
                }
            }

        }

        fun materializeEdges(datasource: HikariDataSource, entitySetIds: Set<UUID>) {
            "DROP MATERIALIZED VIEW IF EXISTS edges"
            "CREATE MATERIALIZED VIEW IF NOT EXISTS edges "
            TODO("MATERIALIZE EDGES")
        }

        @JvmStatic
        fun materializeEntitySets(
                organizationId: UUID,
                authorizedPropertyTypesByEntitySet: Map<UUID, Map<UUID, PropertyType>>
        ): Map<UUID, Set<OrganizationEntitySetFlag>> {
            connect(organizations.getOrganizationPrincipal(organizationId).name).use { datasource ->
                materializeEntitySets(datasource, authorizedPropertyTypesByEntitySet)
            }
            return authorizedPropertyTypesByEntitySet.mapValues { EnumSet.of(OrganizationEntitySetFlag.MATERIALIZED) }
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
            val entitySet = entitySets[entitySetId]!!
            val propertyFqns = authorizedPropertyTypes.mapValues { it.value.type.fullQualifiedNameAsString }
            val sql = selectEntitySetWithCurrentVersionOfPropertyTypes(
                    mapOf(),
                    propertyFqns,
                    authorizedPropertyTypes.values.map(PropertyType::getId),
                    mapOf(entitySetId to authorizedPropertyTypes.keys),
                    mapOf(),
                    EnumSet.allOf(MetadataOption::class.java),
                    authorizedPropertyTypes.mapValues { it.value.datatype == EdmPrimitiveTypeKind.Binary },
                    entitySet.isLinking,
                    entitySet.isLinking
            )
            datasource.connection.use { connection ->
                connection.createStatement().use { stmt ->
                    stmt.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS openlattice.${entitySet.name} AS $sql")
                }
                //Next we need to grant select on materialize view to everyone who has permission.
            }
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
                val dbRole = DataTables.quote(buildSqlRolename(role))

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
            val dbRole = buildSqlRolename(role)

            target.connection.use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute(createRoleIfNotExistsSql(dbRole))
                    //Don't allow users to access public schema which will contain foreign data wrapper tables.
                    statement.execute("REVOKE USAGE ON SCHEMA public FROM ${DataTables.quote(dbRole)}")

                    return@use
                }
            }
        }

        @JvmStatic
        fun createUnprivilegedUser(user: SecurablePrincipal) {
            val dbUser = user.name
            val dbUserPassword = dbCredentialService.getDbCredential(user.name)

            target.connection.use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute(createUserIfNotExistsSql(dbUser, dbUserPassword))
                    //Don't allow users to access public schema which will contain foreign data wrapper tables.
                    statement.execute("REVOKE USAGE ON SCHEMA public FROM ${DataTables.quote(dbUser)}")

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
                    logger.info("Created user mapping. ")
                    statement.execute("IMPORT FOREIGN SCHEMA public FROM SERVER $PRODUCTION INTO public")
                    logger.info("Imported foreign schema")
                }
            }
        }

    }
}

private val PRINCIPALS_SQL = "SELECT acl_key FROM principals WHERE ${PostgresColumn.PRINCIPAL_TYPE.name} = ?"

private val INSERT_MATERIALIZED_ENTITY_SET = "INSERT INTO ${PostgresTable.ORGANIZATION_ASSEMBLIES.name} (?,?) ON CONFLICT DO NOTHING"
private val SELECT_MATERIALIZED_ENTITY_SETS = "SELECT * FROM ${PostgresTable.ORGANIZATION_ASSEMBLIES.name} " +
        "WHERE ${PostgresColumn.ORGANIZATION_ID.name} = ?"

internal fun createOpenlatticeSchema(datasource: HikariDataSource) {
    datasource.connection.use { connection ->
        connection.createStatement().use { statement ->
            statement.execute("CREATE SCHEMA IF NOT EXISTS $SCHEMA")
        }
    }
}

private fun configureUserInDatabase(datasource: HikariDataSource, userId: String) {
    val dbUser = DataTables.quote(userId)

    datasource.connection.use { connection ->
        connection.createStatement().use { statement ->

            statement.execute("GRANT USAGE ON SCHEMA $SCHEMA TO $dbUser")
            //Don't allow users to access public schema which will contain foreign data wrapper tables.
            statement.execute("REVOKE USAGE ON SCHEMA public FROM $dbUser")
            //Set the search path for the user
            statement.execute("ALTER USER $dbUser set search_path TO $SCHEMA")

            return@use
        }
    }
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

internal fun buildUserId(organizationId: UUID): String {
    return "ol-internal|organization|$organizationId"
}

internal fun buildSqlRolename(role: Role): String {
    return "ol-internal|role|${role.id}"
}
