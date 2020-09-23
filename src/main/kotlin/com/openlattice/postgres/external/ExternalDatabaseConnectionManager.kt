package com.openlattice.postgres.external

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.assembler.PostgresDatabases
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.TimeUnit

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class ExternalDatabaseConnectionManager(
        private val assemblerConfiguration: AssemblerConfiguration
) {

    private val perDbCache: LoadingCache<String, HikariDataSource> = CacheBuilder
            .newBuilder()
            .expireAfterAccess(1, TimeUnit.HOURS)
            .build(cacheLoader())

    companion object {
        private val logger = LoggerFactory.getLogger(ExternalDatabaseConnectionManager::class.java)
    }

    fun createOrgDataSource( organizationId: UUID ): HikariDataSource {
        return createDataSource(
                PostgresDatabases.buildOrganizationDatabaseName(organizationId),
                assemblerConfiguration.server.clone() as Properties,
                assemblerConfiguration.ssl
        )
    }

    fun createDataSource(dbName: String, config: Properties, useSsl: Boolean): HikariDataSource {
        config.computeIfPresent("jdbcUrl") { _, jdbcUrl ->
            "${(jdbcUrl as String).removeSuffix("/")}/$dbName" + if (useSsl) {
                "?sslmode=require"
            } else {
                ""
            }
        }
        return HikariDataSource(HikariConfig(config))
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

}