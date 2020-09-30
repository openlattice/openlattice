package com.openlattice.postgres.external

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.openlattice.assembler.AssemblerConfiguration
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.util.*
import java.util.concurrent.TimeUnit

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class ExternalDatabaseConnectionManager(
        private val assemblerConfiguration: AssemblerConfiguration
) {
    companion object {
        @JvmStatic
        private fun buildOrganizationDatabaseName(organizationId: UUID): String {
            return "org_${organizationId.toString().replace("-","").toLowerCase()}"
        }
    }

    private val perDbCache: LoadingCache<String, HikariDataSource> = CacheBuilder
            .newBuilder()
            .expireAfterAccess(1, TimeUnit.HOURS)
            .build(cacheLoader())

    fun createOrgDataSource( organizationId: UUID ): HikariDataSource {
        return createDataSource(
                buildOrganizationDatabaseName(organizationId),
                assemblerConfiguration.server.clone() as Properties,
                assemblerConfiguration.ssl
        )
    }
    fun createDataSource(dbName: String, config: Properties, useSsl: Boolean): HikariDataSource {
        val jdbcUrl = config.getProperty("jdbcUrl")
                ?: throw Exception("No JDBC URL specified in configuration $config")

        val newProps = config.clone() as Properties
        newProps["jdbcUrl"] = appendDatabaseToJdbcPartial(jdbcUrl, dbName) + if (useSsl) {
            "?sslmode=require"
        } else {
            ""
        }
        return HikariDataSource(HikariConfig(newProps))
    }

    private fun cacheLoader(): CacheLoader<String, HikariDataSource> {
        return CacheLoader.from { dbName ->
            createDataSource(
                    dbName!!,
                    assemblerConfiguration.server.clone() as Properties,
                    assemblerConfiguration.ssl
            )
        }
    }

    fun connect(dbName: String): HikariDataSource {
        return perDbCache.get(dbName)
    }

    fun connectOrgDb(organizationId: UUID): HikariDataSource {
        return perDbCache.get(buildOrganizationDatabaseName(organizationId))
    }

    fun appendDatabaseToJdbcPartial( jdbcStringNoDatabase: String, dbName: String ): String {
        return "${jdbcStringNoDatabase.removeSuffix("/")}/$dbName"
    }
}