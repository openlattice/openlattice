package com.openlattice.organizations

import com.openlattice.assembler.AssemblerConfiguration

import com.openlattice.postgres.DataTables.quote
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.util.*

internal class AtlasDataService(
        private val hds: HikariDataSource,
        private val assemblerConfiguration: AssemblerConfiguration //for now using this, may need to make a separate one
) {
    //lifted from assembly connection manager, likely will need to be customized
    companion object {
        @JvmStatic
        fun connect(dbName: String, config: Properties, useSsl: Boolean): HikariDataSource {
            config.computeIfPresent("jdbcUrl") { _, jdbcUrl ->
                "${(jdbcUrl as String).removeSuffix(
                        "/"
                )}/$dbName" + if (useSsl) {
                    "?sslmode=require"
                } else {
                    ""
                }
            }
            return HikariDataSource(HikariConfig(config))
        }
    }

    fun connect(dbName: String): HikariDataSource {
        return connect(dbName, assemblerConfiguration.server.clone() as Properties, assemblerConfiguration.ssl)
    }

    fun grantPermissionsOnAtlastDatabase(dbName: String, tableName: String, ipAddress: String, columnNames: Optional<Set<String>>) {
        connect(dbName).use {
            getGrantSql(tableName, columnNames)
        }
    }

    private fun getGrantSql(tableName: String, columnNames: Optional<Set<String>>): String {
        //make a new username for each ip address?
        val dbOrgUser = quote()
    }

}