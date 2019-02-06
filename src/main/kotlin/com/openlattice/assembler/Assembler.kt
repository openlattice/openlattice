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

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.openlattice.authorization.*
import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.selectEntitySetWithCurrentVersionOfPropertyTypes
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.Organization
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresColumn.PRINCIPAL_TYPE
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import organization.OrganizationEntitySetFlag
import java.sql.ResultSet
import java.util.*
import java.util.function.Function
import java.util.function.Supplier

const val SCHEMA = "openlattice"
const val PRODUCTION = "olprod"
private val logger = LoggerFactory.getLogger(Assembler::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class Assembler(
        private val assemblerConfiguration: AssemblerConfiguration,
        val authz: AuthorizationManager,
        private val dbCredentialService: DbCredentialService,
        val hds: HikariDataSource,
        hazelcastInstance: HazelcastInstance
) {
    private val entitySets: IMap<UUID, EntitySet> = hazelcastInstance.getMap(HazelcastMap.ENTITY_SETS.name)

    private val target = connect("postgres")

    fun initializeRolesAndUsers(spm: SecurePrincipalsManager) {
        getAllRoles(spm).map(this::createRole)
        getAllUsers(spm).map(this::createUnprivilegedUser)
    }


    /**
     * Creates a private organization database that can be used for uploading data using launchpad.
     * Also sets up foreign data wrapper using assembler configuration so that materialized views of data can be
     * provided.
     */
    fun createOrganizationDatabase(organization: Organization, spm: SecurePrincipalsManager) {
        createDatabase(organization.id, organization.principal.id)

        connect(organization.principal.id).use { datasource ->
            configureRolesInDatabase(datasource, spm)
            createOpenlatticeSchema(datasource)

            organization.members.filter { it.id!="openlatticeRole" && it.id!="admin"  }.forEach { principal ->
                configureUserInDatabase(datasource, principal.id)
            }

            createForeignServer(datasource)
            materializePropertyTypes(datasource)
        }
    }


    fun materializeEntityTypes(datasource: HikariDataSource) {
        TODO("Materialize entity types")
    }

    fun materializePropertyTypes(datasource: HikariDataSource) {
        TODO("Materialize property types")
    }

    fun materializeEdges(datasource: HikariDataSource, entitySetIds: Set<UUID>) {
        TODO("MATERIALIZE EDGES")
    }

    fun materializeEntitySets(
            organizationId: UUID,
            organizationPrincipal: Principal,
            authorizedPropertyTypesByEntitySet: Map<UUID, Map<UUID, PropertyType>>
    ): Map<UUID, Set<OrganizationEntitySetFlag>> {
        connect(organizationPrincipal.id).use { datasource ->
            materializeEntitySets(organizationId, datasource, authorizedPropertyTypesByEntitySet)
        }
        return authorizedPropertyTypesByEntitySet.mapValues { EnumSet.of(OrganizationEntitySetFlag.MATERIALIZED) }
    }

    fun materializeEntitySets(
            organizationId: UUID,
            datasource: HikariDataSource,
            authorizedPropertyTypesByEntitySet: Map<UUID, Map<UUID, PropertyType>>
    ) {
        authorizedPropertyTypesByEntitySet.forEach { entitySetId, authorizedPropertyTypes ->
            materialize(organizationId, datasource, entitySetId, authorizedPropertyTypes)
        }

        materializeEdges(datasource, authorizedPropertyTypesByEntitySet.keys)
    }

    /**
     * Materializes an entity set on atlas.
     */
    fun materialize(
            organizationId: UUID, datasource: HikariDataSource, entitySetId: UUID,
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
        hds.connection.use { conn ->
            conn.prepareStatement(INSERT_MATERIALIZED_ENTITY_SET).use { ps ->
                ps.setObject(2, organizationId)
                ps.setObject(2, entitySetId)
                ps.executeUpdate()
            }

        }
    }


    /**
     * Removes a materialized entity set from atlas.
     */
    fun dematerializeEntitySets(datasource: HikariDataSource, entitySetIds: Set<UUID>) {
        TODO("Drop the materialize view for the specified entity sets.")
    }

    fun getMaterializedEntitySets(organizationId: UUID): PostgresIterable<MaterializedEntitySet> {
        return PostgresIterable(
                Supplier {
                    val conn = hds.connection
                    val ps = conn.prepareStatement(SELECT_MATERIALIZED_ENTITY_SETS)
                    ps.setObject(1, organizationId)
                    StatementHolder(conn, ps, ps.executeQuery())
                },
                Function<ResultSet, MaterializedEntitySet> { rs ->
                    ResultSetAdapters.materializedEntitySet(rs)
                }
        )
    }

    private fun createDatabase(organizationId: UUID, dbname: String) {
        val db = quote(dbname)
        val dbRole ="${dbname}_role"
        val unquotedDbAdminUser = buildUserId(organizationId)
        val dbAdminUser = quote(unquotedDbAdminUser)
        val dbAdminUserPassword = dbCredentialService.createUserIfNotExists(unquotedDbAdminUser)
                ?: dbCredentialService.getDbCredential(unquotedDbAdminUser)
        val createDbRole = createRoleIfNotExistsSql(dbRole)
        val createDbUser = createUserIfNotExistsSql(unquotedDbAdminUser, dbAdminUserPassword)
        val grantRole = "GRANT ${quote(dbRole)} TO $dbAdminUser"
        val createDb = " CREATE DATABASE $db WITH OWNER=$dbAdminUser"
        val revokeAll = "REVOKE ALL ON DATABASE $db FROM public"

        //We connect to default db in order to do initial db setup

        target.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(createDbRole)
                statement.execute(createDbUser)
                statement.execute(grantRole)
                if( !exists(dbname) ) {
                    statement.execute(createDb)
                }
                statement.execute(revokeAll)
                return@use
            }
        }

    }

    private fun dropDatabase(organizationId: UUID, dbname: String) {
        val db = quote(dbname)
        val dbRole = quote("${dbname}_role")
        val unquotedDbAdminUser = buildUserId(organizationId)
        val dbAdminUser = quote(unquotedDbAdminUser)
        val dbAdminUserPassword = dbCredentialService.createUser(unquotedDbAdminUser)

        val dropDbUser = "DROP ROLE $dbAdminUser"
        //TODO: If we grant this role to other users, we need to make sure we drop it
        val dropDbRole = "DROP ROLE $dbRole"
        val dropDb = " DROP DATABASE $db"


        //We connect to default db in order to do initial db setup

        target.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(dropDbUser)
                statement.execute(dropDbRole)
                statement.execute(dropDb)
                return@use
            }
        }
    }

    private fun createOpenlatticeSchema(datasource: HikariDataSource) {
        datasource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE SCHEMA IF NOT EXISTS $SCHEMA")
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
            val dbRole = quote(buildSqlRolename(role))

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


    private fun configureUserInDatabase(datasource: HikariDataSource, userId: String) {
        val dbUser = quote(userId)

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

    fun createRole(role: Role) {
        val dbRole = buildSqlRolename(role)

        target.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(createRoleIfNotExistsSql(dbRole))
                //Don't allow users to access public schema which will contain foreign data wrapper tables.
                statement.execute("REVOKE USAGE ON SCHEMA public FROM ${quote(dbRole)}")

                return@use
            }
        }
    }

    fun createUnprivilegedUser(user: SecurablePrincipal) {
        val dbUser = user.name
        val dbUserPassword = dbCredentialService.getDbCredential(user.name)

        target.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(createUserIfNotExistsSql(dbUser, dbUserPassword))
                //Don't allow users to access public schema which will contain foreign data wrapper tables.
                statement.execute("REVOKE USAGE ON SCHEMA public FROM ${quote(dbUser)}")

                return@use
            }
        }
    }

    private fun createForeignServer(datasource: HikariDataSource) {
        logger.info("Setting up foreign server for datasource: {}", datasource.jdbcUrl)
        datasource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE EXTENSION postgres_fdw")

                logger.info("Installed postgres_fdw extension.")

                statement.execute(
                        "CREATE SERVER $PRODUCTION FOREIGN DATA WRAPPER postgres_fdw " +
                                "OPTIONS (host '${assemblerConfiguration.foreignHost}', " +
                                "dbname '${assemblerConfiguration.foreignDbName}', " +
                                "port '${assemblerConfiguration.foreignPort}')"
                )
                logger.info("Created foreign server definition. ")
                statement.execute(
                        "CREATE USER MAPPING FOR CURRENT_USER SERVER $PRODUCTION " +
                                "OPTIONS ( user '${assemblerConfiguration.foreignUsername}', " +
                                "password '${assemblerConfiguration.foreignPassword}')"
                )
                logger.info("Created user mapping. ")
                statement.execute("IMPORT FOREIGN SCHEMA public FROM SERVER $PRODUCTION INTO public")
                logger.info("Imported foreign schema")
            }
        }
    }

    private fun exists( dbname: String ) : Boolean {
        target.connection.use { connection ->
            connection.createStatement().use { stmt ->
                  stmt.executeQuery("select count(*) from pg_database where datname = '$dbname'").use {rs ->
                      rs.next()
                      return rs.getInt("count") > 0
                  }
            }
        }
    }

    private fun connect(dbname: String): HikariDataSource {
        val config = assemblerConfiguration.server.clone() as Properties
        config.computeIfPresent("jdbcUrl") { _, jdbcUrl -> "${(jdbcUrl as String).removeSuffix("/")}/$dbname?ssl=true" }
        return HikariDataSource(HikariConfig(config))
    }

    private fun buildUserId(organizationId: UUID): String {
        return "ol-internal|organization|$organizationId"
    }

    private fun buildSqlRolename(role: Role): String {
        return "ol-internal|role|${role.id}"
    }


}

private fun createRoleIfNotExistsSql(dbRole: String): String {
    return "DO\n" +
            "\$do\$\n" +
            "BEGIN\n" +
            "   IF NOT EXISTS (\n" +
            "      SELECT\n" +
            "      FROM   pg_catalog.pg_roles\n" +
            "      WHERE  rolname = '$dbRole') THEN\n" +
            "\n" +
            "      CREATE ROLE ${quote(dbRole)} NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOLOGIN;\n" +
            "   END IF;\n" +
            "END\n" +
            "\$do\$;"
}

private fun createUserIfNotExistsSql(dbUser: String, dbUserPassword: String): String {
    return "DO\n" +
            "\$do\$\n" +
            "BEGIN\n" +
            "   IF NOT EXISTS (\n" +
            "      SELECT\n" +
            "      FROM   pg_catalog.pg_roles\n" +
            "      WHERE  rolname = '$dbUser') THEN\n" +
            "\n" +
            "      CREATE ROLE ${quote(
                    dbUser
            )} NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN ENCRYPTED PASSWORD '$dbUserPassword';\n" +
            "   END IF;\n" +
            "END\n" +
            "\$do\$;"
}

private val PRINCIPALS_SQL = "SELECT acl_key FROM principals WHERE ${PRINCIPAL_TYPE.name} = ?"

private val INSERT_MATERIALIZED_ENTITY_SET = "INSERT INTO ${PostgresTable.MATERIALIZED_ENTITY_SETS.name} (?,?) ON CONFLICT DO NOTHING"
private val SELECT_MATERIALIZED_ENTITY_SETS = "SELECT * FROM ${PostgresTable.MATERIALIZED_ENTITY_SETS.name} " +
        "WHERE ${PostgresColumn.ORGANIZATION_PRINCIPAL_ID.name} = ?"