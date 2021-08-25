package com.openlattice.postgres

import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.external.Schemas
import com.zaxxer.hikari.HikariDataSource
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.slf4j.LoggerFactory

class PostgresProjectionService {

    companion object {
        private val logger = LoggerFactory.getLogger(PostgresProjectionService::class.java)

        const val RENAME_SERVER_DB_FUNCTION = "rename_server_database"

        // 0 = whole string, 1 = prefix, 2 = hostname, 3 = port, 4 = database
        private val PAT = Regex("""([\w:]+)://([\w_.]*):(\d+)/(\w+)""")

        /**
         * Create FDW between [localSchema] and [remoteDb]
         */
        @SuppressFBWarnings(
                value = ["SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE"],
                justification = "Only internal values provided to SQL update statment"
        )
        fun createFdwBetweenDatabases(
                localDbDatasource: HikariDataSource,
                remoteUser: String,
                remotePassword: String,
                remoteDbJdbc: String,
                localUsername: String,
                localSchema: Schemas,
                fdwName: String
        ) {
            var searchPath = loadSearchPathForCurrentUser(localDbDatasource)
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

        fun changeDbNameForFdw(hds: HikariDataSource, fdwName: String, newDbName: String) {
            hds.connection.use { conn ->
                conn.prepareStatement("SELECT $RENAME_SERVER_DB_FUNCTION(?, ?)").use { ps ->
                    ps.setString(1, fdwName)
                    ps.setString(2, newDbName)
                    ps.execute()
                }
            }
        }

        fun loadSearchPathForCurrentUser(dataSource: HikariDataSource): String {
            logger.debug("loading search path for current user")
            dataSource.connection.use { conn ->
                conn.createStatement().executeQuery("SHOW search_path").use {
                    it.next()
                    val searchPath = it.getString(1)
                    if (searchPath == null) {
                        logger.error("bad search path: {}", searchPath)
                        return ""
                    }
                    return searchPath
                }
            }
        }

        fun importTableFromFdw(
                hds: HikariDataSource,
                fdwName: String,
                sourceSchema: String,
                sourceTableName: String,
                destinationSchema: String,
                destinationTableName: String? = null
        ) {
            hds.connection.use { conn ->
                conn.autoCommit = false

                conn.createStatement().use { stmt ->
                    stmt.execute("""
                                 IMPORT FOREIGN SCHEMA ${quote(sourceSchema)}
                                 LIMIT TO ( ${quote(sourceTableName)} )
                                 FROM SERVER $fdwName 
                                 INTO ${quote(destinationSchema)}
                            """.trimIndent()
                    )

                    if (destinationTableName != null) {
                        stmt.execute(
                                """
                                    ALTER TABLE ${quote(destinationSchema)}.${quote(sourceTableName)}
                                    RENAME TO ${quote(destinationTableName)}
                                """.trimIndent()
                        )
                    }
                }

                conn.autoCommit = true
            }
        }

        fun importTablesFromFdw(
                hds: HikariDataSource,
                fdwName: String,
                sourceSchema: String,
                sourceTableNames: Set<String>,
                destinationSchema: String
        ) {
            val actualTables = filterAlreadyImported(hds, destinationSchema, sourceTableNames)
            if (actualTables.isEmpty()){
                logger.info( "Not importing any tables from fdw, specified tables already imported" )
                return
            }
            if ( actualTables.size != sourceTableNames.size ){
                logger.info( "Some tables already imported, importing ${actualTables.joinToString()}" )
            }
            hds.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute(
                            """
                                 IMPORT FOREIGN SCHEMA ${quote(sourceSchema)}
                                 LIMIT TO ( ${actualTables.joinToString(",") {quote(it)} } )
                                 FROM SERVER $fdwName 
                                 INTO ${quote(destinationSchema)}
                            """.trimIndent()
                    )
                }
            }
        }

        @SuppressFBWarnings(
                value = ["SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE"],
                justification = "Only internal values provided to SQL update statment"
        )
        private fun filterAlreadyImported(
                hds: HikariDataSource,
                destinationSchema: String,
                tablesToImport: Set<String>
        ): Set<String> {
            val existingTables = mutableSetOf<String>()
            hds.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT foreign_table_name FROM information_schema.foreign_tables " +
                            "WHERE foreign_table_schema = '$destinationSchema' " +
                            "AND foreign_table_name = ANY('{${tablesToImport.joinToString { quote(it) } }}')"
                    ).use { rs ->
                        while ( rs.next() ){
                            existingTables.add(rs.getString(1))
                        }
                    }
                }
            }
            return tablesToImport.filterNot { existingTables.contains(it) }.toSet()
        }

        fun dropTableImportedFromFdw(
                hds: HikariDataSource,
                schema: String,
                tableName: String
        ) {
            hds.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("DROP FOREIGN TABLE ${quote(schema)}.${quote(tableName)}")
                }
            }
        }

        fun createViewOverTable(
                hds: HikariDataSource,
                sourceTableSchema: String,
                sourceTableName: String,
                viewSchema: String,
                viewName: String
        ) {
            hds.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("""
                        CREATE OR REPLACE VIEW ${quote(viewSchema)}.${quote(viewName)}
                        AS SELECT * FROM ${quote(sourceTableSchema)}.${quote(sourceTableName)}
                    """.trimIndent())
                }
            }
        }

        fun destroyViewOverTable(
                hds: HikariDataSource,
                viewSchema: String,
                viewName: String
        ) {
            hds.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("DROP VIEW ${quote(viewSchema)}.${quote(viewName)}")
                }
            }
        }
    }

}