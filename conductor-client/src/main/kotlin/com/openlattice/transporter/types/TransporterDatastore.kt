package com.openlattice.transporter.types

import com.geekbeast.configuration.postgres.PostgresConfiguration
import com.kryptnostic.rhizome.configuration.RhizomeConfiguration
import com.openlattice.ApiHelpers
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.authorization.Action
import com.openlattice.edm.EdmConstants
import com.openlattice.edm.EntitySet
import com.openlattice.edm.PropertyTypeIdFqn
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.postgres.external.ExternalDatabasePermissioningService
import com.openlattice.postgres.external.Schemas
import com.openlattice.transporter.*
import com.zaxxer.hikari.HikariDataSource
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.*

/**
 * TransporterDatastore configures entries in [rhizome] postgres configuration
 */
@Component
class TransporterDatastore(
        private val assemblerConfiguration: AssemblerConfiguration,
        rhizome: RhizomeConfiguration,
        private val exConnMan: ExternalDatabaseConnectionManager,
        private val exDbPermMan: ExternalDatabasePermissioningService
) {
    companion object {
        private val logger = LoggerFactory.getLogger(TransporterDatastore::class.java)

        // 0 = whole string, 1 = prefix, 2 = hostname, 3 = port, 4 = database
        private val PAT = Regex("""([\w:]+)://([\w_.]*):(\d+)/(\w+)""")

        // database in atlas where the data is transported
        const val TRANSPORTER_DB_NAME = "transporter"

        // fdw name for atlas <-> production fdw
        const val ENTERPRISE_FDW_NAME = "enterprise"

        private val OPENLATTICE_ID_AS_STRING = EdmConstants.ID_FQN.toString()
    }

    private var transporterHds: HikariDataSource = exConnMan.createDataSource(
            TRANSPORTER_DB_NAME,
            assemblerConfiguration.server.clone() as Properties,
            assemblerConfiguration.ssl
    )

    init {
        logger.info("Initializing TransporterDatastore")
        if (rhizome.postgresConfiguration.isPresent) {
            initializeFDW(rhizome.postgresConfiguration.get())
        }
        val sp = ensureSearchPath(transporterHds)
        if (!sp.contains(Schemas.ENTERPRISE_FDW_SCHEMA.label)) {
            logger.error("bad search path: {}", sp)
        }
    }

    fun transportEntitySet(
            organizationId: UUID,
            es: EntitySet,
            ptIdToFqnColumns: Set<PropertyTypeIdFqn>,
            usersToColumnPermissions: Map<String, List<String>>
    ) {
        val esName = es.name
        val orgHds = exConnMan.connectToOrg(organizationId)

        linkOrgDbToTransporterDb(orgHds, organizationId)

        destroyEdgeViewInOrgDb(orgHds, esName)

        destroyEntitySetViewInOrgDb(orgHds, esName)

        destroyTransportedEntityTypeTableInOrg(orgHds, es.entityTypeId)

        // import et table from foreign server
        transportEdgesTableToOrg(orgHds, organizationId)

        // import edges table from foreign server
        transportTableToOrg(
                orgHds,
                organizationId,
                Schemas.PUBLIC_SCHEMA,
                tableName(es.entityTypeId),
                Schemas.TRANSPORTER_SCHEMA
        )

        // create edge view in org db
        createEdgeViewInOrgDb(
                orgHds,
                esName,
                es.id
        )

        // create view in org db
        createEntitySetViewInOrgDb(
                orgHds,
                es.id,
                esName,
                es.entityTypeId,
                ptIdToFqnColumns
        )

        // create roles, apply permissions
        applyViewAndEdgePermissions(
                orgHds,
                es.id,
                esName,
                usersToColumnPermissions,
                ptIdToFqnColumns
        )
    }

    fun destroyTransportedEntitySet(
            organizationId: UUID,
            orgHds: HikariDataSource = exConnMan.connectToOrg(organizationId),
            entityTypeId: UUID,
            name: String
    ) {
        destroyEdgeViewInOrgDb(orgHds, name)

        destroyEntitySetViewInOrgDb(orgHds, name)

        destroyTransportedEntityTypeTableInOrg(orgHds, entityTypeId)
    }

    private fun linkOrgDbToTransporterDb(
            orgDatasource: HikariDataSource,
            organizationId: UUID
    ) {
        createFdwBetweenDatabases(
                orgDatasource,
                assemblerConfiguration.server.getProperty("username"),
                assemblerConfiguration.server.getProperty("password"),
                exConnMan.appendDatabaseToJdbcPartial(
                        assemblerConfiguration.server.getProperty("jdbcUrl"),
                        TRANSPORTER_DB_NAME
                ),
                assemblerConfiguration.server.getProperty("username"),
                Schemas.TRANSPORTER_SCHEMA,
                constructFdwName(organizationId)
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
            localSchema: Schemas,
            fdwName: String
    ) {
        var searchPath = ensureSearchPath(localDbDatasource)
        if (!searchPath.contains(localSchema.label)) {
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

    private fun ensureSearchPath(dataSource: HikariDataSource): String {
        logger.info("checking search path for current user")
        dataSource.connection.use { conn ->
            conn.createStatement().executeQuery("show search_path").use {
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
    }

    private fun initializeFDW(rhizomeConfig: PostgresConfiguration) {
        createFdwBetweenDatabases(
                transporterHds,
                rhizomeConfig.hikariConfiguration.getProperty("username"),
                rhizomeConfig.hikariConfiguration.getProperty("password"),
                rhizomeConfig.hikariConfiguration.getProperty("jdbcUrl").removeSuffix("?sslmode=require"),
                assemblerConfiguration.server.getProperty("username"),
                Schemas.ENTERPRISE_FDW_SCHEMA,
                ENTERPRISE_FDW_NAME
        )

        transporterHds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("select count(*) from information_schema.foreign_tables where foreign_table_schema = '${Schemas.ENTERPRISE_FDW_SCHEMA}'").use { rs ->
                    if (rs.next() && rs.getInt(1) > 0) {
                        // don't bother if it's already there
                        logger.info("schema already imported, not re-importing")
                    } else {
                        stmt.executeUpdate(
                                importTablesFromForeignSchemaQuery(
                                        Schemas.PUBLIC_SCHEMA,
                                        setOf(
                                                PostgresTable.IDS.name,
                                                PostgresTable.DATA.name,
                                                PostgresTable.E.name
                                        ),
                                        Schemas.ENTERPRISE_FDW_SCHEMA,
                                        ENTERPRISE_FDW_NAME
                                )
                        )
                    }
                }
            }
        }

        transporterHds.close()
        transporterHds = exConnMan.connect(TRANSPORTER_DB_NAME)
    }

    fun datastore(): HikariDataSource {
        return transporterHds
    }

    private fun constructFdwName(organizationId: UUID): String {
        return ApiHelpers.dbQuote("fdw_$organizationId")
    }

    fun destroyTransportedEntityTypeTableInOrg(
            orgDatasource: HikariDataSource,
            entityTypeId: UUID
    ) {
        orgDatasource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate(dropForeignTypeTable(Schemas.TRANSPORTER_SCHEMA, entityTypeId))
            }
        }
    }

    fun destroyEntitySetViewInOrgDb(
            orgDatasource: HikariDataSource,
            entitySetName: String
    ) {
        orgDatasource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate(destroyView(Schemas.ASSEMBLED_ENTITY_SETS, entitySetName))
            }
        }
    }

    fun destroyEdgeViewInOrgDb(
            orgDatasource: HikariDataSource,
            entitySetName: String) {
        orgDatasource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate(destroyEdgeView(Schemas.ASSEMBLED_ENTITY_SETS, entitySetName))
            }
        }
    }

    private fun createEdgeViewInOrgDb(
            orgDatasource: HikariDataSource,
            entitySetName: String,
            entitySetId: UUID
    ) {
        orgDatasource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate(
                        createEdgeSetViewInSchema(
                                entitySetName,
                                entitySetId,
                                Schemas.ASSEMBLED_ENTITY_SETS,
                                Schemas.TRANSPORTER_SCHEMA
                        )
                )
            }
        }
    }

    private fun createEntitySetViewInOrgDb(
            orgDatasource: HikariDataSource,
            entitySetId: UUID,
            entitySetName: String,
            entityTypeId: UUID,
            ptIdToFqnColumns: Set<PropertyTypeIdFqn>
    ) {
        orgDatasource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate(
                        createEntitySetViewInSchemaFromSchema(
                                entitySetName,
                                entitySetId,
                                Schemas.ASSEMBLED_ENTITY_SETS,
                                entityTypeId,
                                ptIdToFqnColumns,
                                Schemas.TRANSPORTER_SCHEMA
                        )
                )
            }
        }
    }

    private fun applyViewAndEdgePermissions(
            orgDatasource: HikariDataSource,
            entitySetId: UUID,
            entitySetName: String,
            usersToColumnPermissions: Map<String, List<String>>,
            ptIdToFqnColumns: Set<PropertyTypeIdFqn>
    ) {
        exDbPermMan.initializeAssemblyPermissions(
                orgDatasource,
                entitySetId,
                entitySetName,
                ptIdToFqnColumns.toSet()
        )

        exDbPermMan.updateAssemblyPermissions(
                Action.SET,
                listOf(), // Hmmm
                mapOf() // larger hmmmm
        )

        orgDatasource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                usersToColumnPermissions.forEach { (username, _) ->
                    stmt.execute(setUserInhertRolePrivileges(username))
                    stmt.execute(grantUsageOnschemaSql(Schemas.ASSEMBLED_ENTITY_SETS, username))
                }

                val allPermissions = mutableSetOf<String>()
                usersToColumnPermissions.forEach { (username, allowedCols) ->
                    logger.debug("user {} has columns {}", username, allowedCols)
                    stmt.execute(setUserInhertRolePrivileges(username))
                    stmt.execute(grantUsageOnschemaSql(Schemas.ASSEMBLED_ENTITY_SETS, username))
                    stmt.execute(revokeTablePermissionsForRole(Schemas.ASSEMBLED_ENTITY_SETS, entitySetName, username))

                    allPermissions.addAll(allowedCols)
                }

                allPermissions.forEach { columnName ->
                    val roleName = viewRoleName(entitySetName, columnName)
//                    stmt.execute(exDbPermMan.c)
                    stmt.execute(revokeTablePermissionsForRole(Schemas.ASSEMBLED_ENTITY_SETS, entitySetName, roleName))
                    stmt.execute(grantUsageOnschemaSql(Schemas.ASSEMBLED_ENTITY_SETS, roleName))
                    // grant openlattice.@id cølumn explicitly
                    stmt.execute(grantSelectOnColumnsToRoles(Schemas.ASSEMBLED_ENTITY_SETS, entitySetName, roleName, listOf(columnName, OPENLATTICE_ID_AS_STRING)))
                    stmt.execute(grantSelectOnColumnsToRoles(Schemas.ASSEMBLED_ENTITY_SETS, edgeViewName(entitySetName), roleName, MAT_EDGES_COLUMNS_LIST))
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
            orgDatasource: HikariDataSource,
            organizationId: UUID,
            fromSchema: Schemas,
            fromTableName: String,
            toSchema: Schemas
    ) {
        orgDatasource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate(
                        importTablesFromForeignSchemaQuery(
                                fromSchema,
                                setOf(fromTableName),
                                toSchema,
                                constructFdwName(organizationId)
                        )
                )
            }
        }
    }

    private fun checkIfTableExists(
            orgDatasource: HikariDataSource,
            schema: Schemas,
            tableName: String
    ): Boolean {
        orgDatasource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery(checkIfTableExistsQuery(schema, tableName)).use { rs ->
                    return rs.next() && rs.getBoolean(1)
                }
            }
        }
    }

    private fun transportEdgesTableToOrg(
            orgDatasource: HikariDataSource,
            organizationId: UUID
    ) {
        val edgesTableExists = checkIfTableExists(
                orgDatasource,
                Schemas.TRANSPORTER_SCHEMA,
                MAT_EDGES_TABLE_NAME
        )
        if (edgesTableExists) {
            return
        }

        transportTableToOrg(
                orgDatasource,
                organizationId,
                Schemas.PUBLIC_SCHEMA,
                MAT_EDGES_TABLE_NAME,
                Schemas.TRANSPORTER_SCHEMA
        )
    }
}