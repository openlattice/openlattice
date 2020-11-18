package com.openlattice.postgres.external

import com.google.common.eventbus.EventBus
import com.google.common.eventbus.Subscribe
import com.openlattice.ApiHelpers
import com.openlattice.assembler.PostgresRoles
import com.openlattice.authorization.DbCredentialService
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.DataTables
import com.openlattice.principals.RoleCreatedEvent
import com.openlattice.principals.UserCreatedEvent
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory

/**
 * @author Drew Bailey (drewbaileym@gmail.com)
 */
class ExternalDatabasePermissionsManager(
        private val extDbManager: ExternalDatabaseConnectionManager,
        private val dbCredentialService: DbCredentialService,
        private val securePrincipalsManager: SecurePrincipalsManager,
        eventBus: EventBus
) {
    private val atlas: HikariDataSource = extDbManager.connect("postgres")

    init {
        eventBus.register(this)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ExternalDatabasePermissionsManager::class.java)
    }

    @Subscribe
    fun handleRoleCreated(roleCreatedEvent: RoleCreatedEvent) {
        createRole(roleCreatedEvent.role)
    }

    @Subscribe
    fun handleUserCreated(userCreatedEvent: UserCreatedEvent) {
        createUnprivilegedUser(userCreatedEvent.user)
    }

    internal fun configureRolesInDatabase(dataSource: HikariDataSource) {
        val roles = securePrincipalsManager.allRoles

        if (roles.isNotEmpty()) {
            val roleIds = roles.map { PostgresRoles.buildPostgresRoleName(it) }
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

    internal fun createRole(role: Role) {
        val dbRole = PostgresRoles.buildPostgresRoleName(role)

        atlas.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(createRoleIfNotExistsSql(dbRole))
                //Don't allow users to access public schema which will contain foreign data wrapper tables.
                logger.info("Revoking ${Schemas.PUBLIC_SCHEMA} schema right from role: {}", role)
                statement.execute("REVOKE USAGE ON SCHEMA ${Schemas.PUBLIC_SCHEMA} FROM ${DataTables.quote(dbRole)}")
            }
        }
    }

    internal fun createUnprivilegedUser(user: SecurablePrincipal) {
        /**
         * To simplify work-around for ESRI username limitations, we are only introducing one additional
         * field into the dbcreds table. We keep the results of calling [buildPostgresUsername] as the lookup
         * key, but instead use the username and password returned from the db credential service.
         */
        val (dbUser, dbUserPassword) = dbCredentialService.getOrCreateUserCredentials(user)

        atlas.connection.use { connection ->
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
                logger.info("Revoking ${Schemas.PUBLIC_SCHEMA} schema right from user {}", user)
                statement.execute("REVOKE USAGE ON SCHEMA ${Schemas.PUBLIC_SCHEMA} FROM ${DataTables.quote(dbUser)}")
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
                "      CREATE ROLE ${ApiHelpers.dbQuote(dbRole)} NOSUPERUSER NOCREATEDB NOCREATEROLE NOLOGIN;\n" +
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
}