package com.openlattice.transporter.types

import com.geekbeast.configuration.postgres.PostgresConfiguration
import com.kryptnostic.rhizome.configuration.RhizomeConfiguration
import com.openlattice.ApiUtil
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.assembler.PostgresDatabases
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.zaxxer.hikari.HikariDataSource
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
        private val exConnMan: ExternalDatabaseConnectionManager
) {
    companion object {
        private val logger = LoggerFactory.getLogger(TransporterDatastore::class.java)
        private val PAT = Regex("""([\w:]+)://([\w_.]*):(\d+)/(\w+)""")

        const val ORG_VIEWS_SCHEMA = "transporter"
    }

    private val enterpriseFdwName = "enterprise"
    private val enterpriseFdwSchema = "ol"
    private var hds: HikariDataSource = exConnMan.createDataSource(
            "transporter",
            assemblerConfiguration.server.clone() as Properties,
            assemblerConfiguration.ssl
    )

    init {
        logger.info("Initializing TransporterDatastore")
        if (rhizome.postgresConfiguration.isPresent) {
            initializeFDW(rhizome.postgresConfiguration.get())
        }
        val sp = ensureSearchPath(hds)
        if ( !sp.contains( enterpriseFdwSchema )) {
            logger.error("bad search path: {}", sp)
        }
    }

    fun linkOrgDbToTransporterDb( organizationId: UUID ) {
        val hds = createOrgDataSource( organizationId )
        createFdwBetweenDatabases(
                hds,
                assemblerConfiguration.server.getProperty("username"),
                assemblerConfiguration.server.getProperty("password"),
                exConnMan.appendDatabaseToJdbcPartial(
                        assemblerConfiguration.server.getProperty("jdbcUrl"),
                        "transporter"
                ),
                assemblerConfiguration.server.getProperty("username"),
                ORG_VIEWS_SCHEMA,
                getOrgFdw( organizationId )
        )
    }

    /**
     * Create FDW between [localSchema] and [remoteDb]
     */
    private fun createFdwBetweenDatabases(
            localDbDatasource: HikariDataSource,
            remoteUser: String,
            remotePassword: String,
            remoteDbJdbc: String,
            localUsername: String,
            localSchema: String,
            fdwName: String
    ) {
        var searchPath = ensureSearchPath(localDbDatasource)
        if ( !searchPath.contains(localSchema) ){
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

            val match = PAT.matchEntire(remoteDbJdbc) ?: throw IllegalArgumentException("Invalid jdbc url: $remoteDbJdbc")
            // 0 = whole string, 1 = prefix, 2 = hostname, 3 = port, 4 = database
            val remoteHostname = match.groupValues[2]
            val remotePort = match.groupValues[3].toInt()
            val remoteDbname = match.groupValues[4]
            logger.info("Configuring fdw from {} to {}", localDbDatasource.jdbcUrl, remoteDbJdbc)

            val queries = """
                |create extension if not exists postgres_fdw;
                |create server if not exists $fdwName foreign data wrapper postgres_fdw options (host '$remoteHostname', dbname '$remoteDbname', port '$remotePort');
                |create user mapping if not exists for $localUsername server $fdwName options (user '$remoteUser', password '$remotePassword');
                |create schema if not exists $localSchema;
                |alter user $localUsername set search_path to $searchPath;
                |set search_path to $searchPath;
            """.trimMargin()
                    .split("\n")

            queries.forEach {
                logger.info("about to run {}", it)
            }
            queries.forEach { sql ->
                logger.info("running {}", sql)
                st.execute(sql)
            }
            conn.commit()
        }
    }

    fun importTablesFromForeignSchema(
            remoteSchema: String,
            remoteTables: Set<String>,
            localSchema: String,
            usingFdwName: String
    ): String {
        val tablesClause = if ( remoteTables.isEmpty() ){
            ""
        } else {
            "LIMIT TO (${remoteTables.joinToString(",")})"
        }
        return "import foreign schema $remoteSchema $tablesClause from server $usingFdwName INTO $localSchema;"
    }

    private fun ensureSearchPath( hds: HikariDataSource ): String {
        logger.info("checking search path for current user")
        hds.connection.use { conn ->
            val st = conn.createStatement()
            st.executeQuery( "show search_path" ).use {
                it.next()
                val searchPath = it.getString(1)
                logger.info(searchPath)
                if ( searchPath == null ) {
                    logger.error("bad search path: {}", searchPath)
                    return ""
                }
                return searchPath
            }
        }
    }

    private fun initializeFDW(rhizomeConfig: PostgresConfiguration) {
        createFdwBetweenDatabases(
                hds,
                rhizomeConfig.hikariConfiguration.getProperty("username"),
                rhizomeConfig.hikariConfiguration.getProperty("password"),
                rhizomeConfig.hikariConfiguration.getProperty("jdbcUrl"),
                assemblerConfiguration.server.getProperty("username"),
                enterpriseFdwSchema,
                enterpriseFdwName
        )

        hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("select count(*) from information_schema.foreign_tables where foreign_table_schema = 'ol'").use { rs ->
                    if (rs.next() && rs.getInt(1) > 0) {
                        // don't bother if it's already there
                        logger.info("schema already imported, not re-importing")
                    } else {
                        stmt.executeUpdate(
                                importTablesFromForeignSchema(
                                        "public",
                                        setOf(),
                                        enterpriseFdwSchema,
                                        enterpriseFdwName
                                )
                        )
                    }
                }
            }
        }

        hds.close()
        hds = exConnMan.connect("transporter")
    }

    fun datastore(): HikariDataSource {
        return hds
    }

    fun createOrgDataSource(organizationId: UUID): HikariDataSource {
        return exConnMan.connect( PostgresDatabases.buildOrganizationDatabaseName( organizationId ) )
    }

    fun getOrgFdw( organizationId: UUID ): String {
        return ApiUtil.dbQuote("fdw_$organizationId")
    }

}