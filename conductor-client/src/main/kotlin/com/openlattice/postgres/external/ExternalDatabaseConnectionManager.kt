package com.openlattice.postgres.external

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.hazelcast.core.HazelcastInstance
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.transporter.types.TransporterDatastore
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.util.*

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class ExternalDatabaseConnectionManager(
        private val assemblerConfiguration: AssemblerConfiguration,
        hazelcastInstance: HazelcastInstance
) {
    private val organizationDatabases = HazelcastMap.ORGANIZATION_DATABASES.getMap(hazelcastInstance)

    companion object {
        private val logger = LoggerFactory.getLogger(ExternalDatabaseConnectionManager::class.java)

        fun buildDefaultOrganizationDatabaseName(organizationId: UUID): String {
            return "org_${organizationId.toString().replace("-", "").toLowerCase()}"
        }

        fun buildDefaultCollaborationDatabaseName(collaborationId: UUID): String {
            return "collab_${collaborationId.toString().replace("-", "").toLowerCase()}"
        }
    }

    private val perDbCache: LoadingCache<String, HikariDataSource> = CacheBuilder
            .newBuilder()
            .expireAfterAccess(Duration.ofHours(1))
            .removalListener<String, HikariDataSource> { it.value.close() }
            .build(cacheLoader())

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
                    assemblerConfiguration.server,
                    assemblerConfiguration.ssl
            )
        }
    }

    fun getDatabaseNameOrDefault(databaseId: UUID, extDbType: ExternalDatabaseType): String {
        val maybeOrg = organizationDatabases[databaseId]?: return when(extDbType) {
            ExternalDatabaseType.COLLABORATION -> buildDefaultCollaborationDatabaseName(databaseId)
            ExternalDatabaseType.ORGANIZATION -> buildDefaultOrganizationDatabaseName(databaseId)
        }
        return maybeOrg.name
    }

    fun getDatabaseName(databaseId: UUID): String {
        val maybeOrg = organizationDatabases[databaseId]

        requireNotNull(maybeOrg ) {
            logger.error("Database {} does not exist in the organizationDatabases mapstore", databaseId)
            "Database {} does not exist in the organizationDatabases mapstore"
        }
        return maybeOrg.name
    }

    fun deleteOrganizationDatabase(organizationId: UUID) {
        organizationDatabases.delete(organizationId)
    }

    fun connectToTransporter(): HikariDataSource {
        return perDbCache.get(TransporterDatastore.TRANSPORTER_DB_NAME)
    }

    fun connectAsSuperuser(): HikariDataSource {
        return perDbCache.get("postgres")
    }

    fun connectToOrg(organizationId: UUID): HikariDataSource {
        return perDbCache.get(getDatabaseName(organizationId))
    }

    fun connectToOrgGettingName(organizationId: UUID): Pair<HikariDataSource, String> {
        val orgName = getDatabaseName(organizationId)
        return perDbCache.get(orgName) to orgName
    }

    fun connectToDatabaseOrDefault(databaseId: UUID, extDbType: ExternalDatabaseType): Pair<HikariDataSource, String> {
        val dbName = getDatabaseNameOrDefault(databaseId, extDbType)
        return perDbCache.get(dbName) to dbName
    }

    fun appendDatabaseToJdbcPartial(jdbcStringNoDatabase: String, dbName: String): String {
        return "${jdbcStringNoDatabase.removeSuffix("/")}/$dbName"
    }
}

enum class ExternalDatabaseType {
    COLLABORATION, ORGANIZATION
}