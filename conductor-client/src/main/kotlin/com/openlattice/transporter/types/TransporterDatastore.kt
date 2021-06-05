package com.openlattice.transporter.types

import com.geekbeast.configuration.postgres.PostgresConfiguration
import com.kryptnostic.rhizome.configuration.RhizomeConfiguration
import com.openlattice.ApiHelpers
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.assembler.createRoleIfNotExistsSql
import com.openlattice.edm.EdmConstants
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.transporter.*
import com.zaxxer.hikari.HikariDataSource
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.sql.Connection
import java.util.*

/**
 * TransporterDatastore configures entries in [rhizome] postgres configuration
 */
@Component
class TransporterDatastore(
        private val assemblerConfiguration: AssemblerConfiguration,
        rhizome: RhizomeConfiguration,
        private val exConnMan: ExternalDatabaseConnectionManager
) {
    companion object {
        private val logger = LoggerFactory.getLogger(TransporterDatastore::class.java)

        // 0 = whole string, 1 = prefix, 2 = hostname, 3 = port, 4 = database
        private val PAT = Regex("""([\w:]+)://([\w_.]*):(\d+)/(\w+)""")

        // schema in org_* database where the view is projected
        const val ORG_VIEWS_SCHEMA = "entitysets"

        // schema in org_* database where the foregin tables are accessible
        const val ORG_FOREIGN_TABLES_SCHEMA = "transporter"

        // database in atlas where the data is transported
        const val TRANSPORTER_DB_NAME = "transporter"

        // schema in atlas where views live
        const val PUBLIC_SCHEMA = "public"

        // schema in atlas where production tables are accessible
        const val ENTERPRISE_FDW_SCHEMA = "ol"

        // fdw name for atlas <-> production fdw
        const val ENTERPRISE_FDW_NAME = "enterprise"

        private val OPENLATTICE_ID_AS_STRING = EdmConstants.ID_FQN.toString()
    }

    private var hds: HikariDataSource = exConnMan.createDataSource(
            TRANSPORTER_DB_NAME,
            assemblerConfiguration.server.clone() as Properties,
            assemblerConfiguration.ssl
    )

    init {
        logger.info("Initializing TransporterDatastore")
        if (rhizome.postgresConfiguration.isPresent) {
            initializeFDW(rhizome.postgresConfiguration.get())
        }
        val sp = ensureSearchPath(hds.connection)
        if (!sp.contains(ENTERPRISE_FDW_SCHEMA)) {
            logger.error("bad search path: {}", sp)
        }
    }

    fun linkOrgDbToTransporterDb(organizationId: UUID) {

        createFdwBetweenDatabases(
                connectOrgDb(organizationId),
                assemblerConfiguration.server.getProperty("username"),
                assemblerConfiguration.server.getProperty("password"),
                exConnMan.appendDatabaseToJdbcPartial(
                        assemblerConfiguration.server.getProperty("jdbcUrl"),
                        TRANSPORTER_DB_NAME
                ),
                assemblerConfiguration.server.getProperty("username"),
                ORG_FOREIGN_TABLES_SCHEMA,
                getOrgFdw(organizationId)
        )
    }

    /**
     * Create FDW between [localSchema] and [remoteDb]
     */
    @SuppressFBWarnings(
            value = ["SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE"],
            justification = "Only internal values provided to SQL update statment"
    )
    private fun createFdwBetweenDatabases(
            localDbDatasource: HikariDataSource,
            remoteUser: String,
            remotePassword: String,
            remoteDbJdbc: String,
            localUsername: String,
            localSchema: String,
            fdwName: String
    ) {
        var searchPath = ensureSearchPath(localDbDatasource.connection)
        if (!searchPath.contains(localSchema)) {
            searchPath = "$searchPath, $localSchema"
        }

        localDbDatasource.connection.use { conn ->
            conn.autoCommit = false
            val st = conn.createStatement()
            st.executeQuery("select count(*) from information_schema.foreign_tables where foreign_table_schema = '$localSchema'").use { rs ->
                if (rs.next() && rs.getInt(1) > 0) {
                    // don't bother if it's already there
                    logger.info("fdw already exists, not re-creating")
                    return
                }
            }

            val match = PAT.matchEntire(remoteDbJdbc)
                    ?: throw IllegalArgumentException("Invalid jdbc url: $remoteDbJdbc")
            val remoteHostname = match.groupValues[2]
            val remotePort = match.groupValues[3].toInt()
            val remoteDbname = match.groupValues[4]
            logger.info("Configuring fdw from {} to {}", localDbDatasource.jdbcUrl, remoteDbJdbc)

            """
                |create extension if not exists postgres_fdw;
                |create server if not exists $fdwName foreign data wrapper postgres_fdw options (host '$remoteHostname', dbname '$remoteDbname', port '$remotePort');
                |create user mapping if not exists for $localUsername server $fdwName options (user '$remoteUser', password '$remotePassword');
                |create schema if not exists $localSchema;
                |alter user $localUsername set search_path to $searchPath;
                |set search_path to $searchPath;
            """.trimMargin()
                    .split("\n")
                    .forEach { sql ->
                        logger.info("running {}", sql)
                        st.execute(sql)
                    }
            conn.commit()
        }
    }

    private fun ensureSearchPath(connection: Connection): String {
        logger.info("checking search path for current user")
        connection.createStatement().executeQuery("show search_path").use {
            it.next()
            val searchPath = it.getString(1)
            logger.info(searchPath)
            if (searchPath == null) {
                logger.error("bad search path: {}", searchPath)
                return ""
            }
            return searchPath
        }
    }

    private fun initializeFDW(rhizomeConfig: PostgresConfiguration) {
        createFdwBetweenDatabases(
                hds,
                rhizomeConfig.hikariConfiguration.getProperty("username"),
                rhizomeConfig.hikariConfiguration.getProperty("password"),
                rhizomeConfig.hikariConfiguration.getProperty("jdbcUrl").removeSuffix("?sslmode=require"),
                assemblerConfiguration.server.getProperty("username"),
                ENTERPRISE_FDW_SCHEMA,
                ENTERPRISE_FDW_NAME
        )

        hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("select count(*) from information_schema.foreign_tables where foreign_table_schema = '$ENTERPRISE_FDW_SCHEMA'").use { rs ->
                    if (rs.next() && rs.getInt(1) > 0) {
                        // don't bother if it's already there
                        logger.info("schema already imported, not re-importing")
                    } else {
                        stmt.executeUpdate(
                                importTablesFromForeignSchemaQuery(
                                        PUBLIC_SCHEMA,
                                        setOf(
                                                PostgresTable.IDS.name,
                                                PostgresTable.DATA.name,
                                                PostgresTable.E.name
                                        ),
                                        ENTERPRISE_FDW_SCHEMA,
                                        ENTERPRISE_FDW_NAME
                                )
                        )
                    }
                }
            }
        }

        hds.close()
        hds = exConnMan.connect(TRANSPORTER_DB_NAME)
    }

    fun datastore(): HikariDataSource {
        return hds
    }

    fun connectOrgDb(organizationId: UUID): HikariDataSource {
        return exConnMan.connectToOrg(organizationId)
    }

    fun getOrgFdw(organizationId: UUID): String {
        return ApiHelpers.dbQuote("fdw_$organizationId")
    }

    fun destroyTransportedEntitySetFromOrg(organizationId: UUID, entitySetName: String) {
        connectOrgDb(organizationId).connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate(destroyView(ORG_FOREIGN_TABLES_SCHEMA, entitySetName))
            }
        }
    }

    fun destroyTransportedEntityTypeTableInOrg(organizationId: UUID, entityTypeId: UUID) {
        connectOrgDb(organizationId).connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate(dropForeignTypeTable(ORG_FOREIGN_TABLES_SCHEMA, entityTypeId))
            }
        }
    }

    fun destroyEntitySetViewFromTransporter(entitySetName: String) {
        datastore().connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate(destroyView(PUBLIC_SCHEMA, entitySetName))
            }
        }
    }

    fun destroyEntitySetViewInOrgDb(organizationId: UUID, entitySetName: String) {
        connectOrgDb(organizationId).connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate(destroyView(ORG_VIEWS_SCHEMA, entitySetName))
            }
        }
    }

    fun destroyEdgeViewInOrgDb(organizationId: UUID, entitySetName: String) {
        connectOrgDb(organizationId).connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate(destroyEdgeView(ORG_VIEWS_SCHEMA, entitySetName))
            }
        }
    }

    fun createEdgeViewInOrgDb(
            organizationId: UUID,
            entitySetName: String,
            entitySetId: UUID
    ) {
        connectOrgDb(organizationId).connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate(
                        createEdgeSetViewInSchema(
                                entitySetName,
                                entitySetId,
                                ORG_VIEWS_SCHEMA,
                                ORG_FOREIGN_TABLES_SCHEMA
                        )
                )
            }
        }
    }

    fun createEntitySetViewInOrgDb(
            organizationId: UUID,
            entitySetName: String,
            entitySetId: UUID,
            entityTypeId: UUID,
            ptIdToFqnColumns: Map<UUID, FullQualifiedName>
    ) {
        connectOrgDb(organizationId).connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate(
                        createEntitySetViewInSchemaFromSchema(
                                entitySetName,
                                entitySetId,
                                ORG_VIEWS_SCHEMA,
                                entityTypeId,
                                ptIdToFqnColumns,
                                ORG_FOREIGN_TABLES_SCHEMA
                        )
                )
            }
        }
    }

    fun applyViewAndEdgePermissions(
            organizationId: UUID,
            entitySetName: String,
            usersToColumnPermissions: Map<String, List<String>>
    ) {
        connectOrgDb(organizationId).connection.use { conn ->
            conn.createStatement().use { stmt ->
                val allPermissions = mutableSetOf<String>()
                usersToColumnPermissions.forEach { (username, allowedCols) ->
                    logger.debug("user {} has columns {}", username, allowedCols)
                    stmt.execute(setUserInhertRolePrivileges(username))
                    stmt.execute(grantUsageOnschemaSql(ORG_VIEWS_SCHEMA, username))
                    stmt.execute(revokeTablePermissionsForRole(ORG_VIEWS_SCHEMA, entitySetName, username))

                    allPermissions.addAll(allowedCols)
                }

                allPermissions.forEach { columnName ->
                    val roleName = viewRoleName(entitySetName, columnName)
                    stmt.execute(createRoleIfNotExistsSql(roleName))
                    stmt.execute(grantUsageOnschemaSql(ORG_VIEWS_SCHEMA, roleName))
                    stmt.execute(revokeTablePermissionsForRole(ORG_VIEWS_SCHEMA, entitySetName, roleName))
                    // grant openlattice.@id cølumn explicitly
                    stmt.execute(grantSelectOnColumnsToRoles(ORG_VIEWS_SCHEMA, entitySetName, roleName, listOf(columnName, OPENLATTICE_ID_AS_STRING)))
                    stmt.execute(grantSelectOnColumnsToRoles(ORG_VIEWS_SCHEMA, edgeViewName(entitySetName), roleName, MAT_EDGES_COLUMNS_LIST))
                }
            }

            conn.createStatement().use { batch ->
                // TODO: invalidate/update this when pt types and permissions are changed
                usersToColumnPermissions.forEach { (username, allowedCols) ->
                    allowedCols.forEach { column ->
                        batch.addBatch(grantRoleToUser(viewRoleName(entitySetName, column), username))
                    }
                }
                batch.executeBatch()
            }
        }
    }

    private fun transportTableToOrg(
            organizationId: UUID,
            fromSchemaName: String,
            fromTableName: String,
            toSchemaName: String
    ) {
        connectOrgDb(organizationId).connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate(
                        importTablesFromForeignSchemaQuery(
                                fromSchemaName,
                                setOf(fromTableName),
                                toSchemaName,
                                getOrgFdw(organizationId)
                        )
                )
            }
        }
    }

    private fun checkIfTableExists(
        organizationId: UUID,
        schemaName: String,
        tableName: String
    ): Boolean {
        connectOrgDb(organizationId).connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery(checkIfTableExistsQuery(schemaName, tableName)).use { rs ->
                    return rs.next() && rs.getBoolean(1)
                }
            }
        }
    }

    fun transportEdgesTableToOrg(organizationId: UUID) {
        val edgesTableExists = checkIfTableExists(
            organizationId,
            ORG_FOREIGN_TABLES_SCHEMA,
            MAT_EDGES_TABLE_NAME
        )
        if (edgesTableExists) {
            return
        }

        transportTableToOrg(
                organizationId,
                PUBLIC_SCHEMA,
                MAT_EDGES_TABLE_NAME,
                ORG_FOREIGN_TABLES_SCHEMA
        )
    }

    fun transportEntityTypeTableToOrg(
            organizationId: UUID,
            entityTypeId: UUID
    ) {
        transportTableToOrg(
                organizationId,
                PUBLIC_SCHEMA,
                tableName(entityTypeId),
                ORG_FOREIGN_TABLES_SCHEMA
        )
    }
}