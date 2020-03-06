package com.openlattice.transporter.types

import com.geekbeast.configuration.postgres.PostgresConfiguration
import com.google.common.base.Strings
import com.kryptnostic.rhizome.configuration.RhizomeConfiguration
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.postgresql.util.PSQLException
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.sql.Statement

@Component
class TransporterDatastore(private val configuration: TransporterConfiguration, rhizome: RhizomeConfiguration) {
    companion object {
        private val logger = LoggerFactory.getLogger(TransporterDatastore::class.java)
        private val PAT = Regex("""([\w:]+)://([\w_.]*):(\d+)/(\w+)""")
    }
    private val fdwName = "enterprise"
    private val fdwSchema = "ol"
    private val hds: HikariDataSource = HikariDataSource(HikariConfig(configuration.server))

    init {
        if (rhizome.postgresConfiguration.isPresent) {
            initializeFDW(rhizome.postgresConfiguration.get())
        }
    }

    private fun initializeFDW(enterprise: PostgresConfiguration) {
        hds.connection.use { conn ->
            conn.autoCommit = false
            val st = conn.createStatement()
            st.executeQuery("select count(*) from information_schema.foreign_tables where foreign_table_schema = '$fdwSchema'").use { rs ->
                if (rs.next() && rs.getInt(1) > 0) {
                    // don't bother if it's already there
                    return
                }
            }
            val url = enterprise.hikariConfiguration.getProperty("jdbcUrl")
            val match = PAT.matchEntire(url) ?: throw IllegalArgumentException("Invalid jdbc url: $url")
            // 0 = whole string, 1 = prefix, 2 = hostname, 3 = port, 4 = database
            val hostname = match.groupValues[2]
            val port = match.groupValues[3].toInt()
            val dbname = match.groupValues[4]
            val user = configuration.server.getProperty("username")
            val remoteUser = enterprise.hikariConfiguration.getProperty("username")
            val remotePassword = enterprise.hikariConfiguration.getProperty("password")
            """
                |create extension if not exists postgres_fdw;
                |create server if not exists $fdwName foreign data wrapper postgres_fdw options (host '$hostname', dbname '$dbname', port '$port');
                |create user mapping if not exists for $user server $fdwName options (user '$remoteUser', password '$remotePassword');
                |create schema if not exists $fdwSchema;
                |import foreign schema public from server $fdwName INTO $fdwSchema;
                |alter user $user set search_path to public, $fdwSchema;
            """
                    .trimMargin()
                    .split("\n")
                    .forEach { sql ->
                        st.execute(sql)
                    }
            conn.commit()
        }
    }

    fun datastore(): HikariDataSource {
        return hds
    }
}