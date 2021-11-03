package com.openlattice.postgres.external

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.hazelcast.core.HazelcastInstance
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organizations.JdbcConnectionParameters
import com.openlattice.organizations.OrganizationWarehouse
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*


/**
 * @author Andrew Carter andrew@openlattice.com
 */
class ExternalWarehouseConnectionManager(
    hazelcastInstance: HazelcastInstance
) {
    private val organizationWarehouses = HazelcastMap.ORGANIZATION_WAREHOUSES.getMap(hazelcastInstance)
    private val warehouses = HazelcastMap.WAREHOUSES.getMap(hazelcastInstance)

    companion object {
        private val logger = LoggerFactory.getLogger(ExternalWarehouseConnectionManager::class.java)
    }

    private val perDbCache: LoadingCache<String, HikariDataSource> = CacheBuilder
        .newBuilder()
        .expireAfterAccess(Duration.ofHours(1))
        .removalListener<String, HikariDataSource> { it.value.close() }
        .build(cacheLoader())

    fun createDataSource(organizationId: UUID, useSsl: Boolean): HikariDataSource {
        val maybeOrganizationWarehouse = organizationWarehouses.getValue(organizationId)
        requireNotNull(maybeOrganizationWarehouse) {
            throw Exception("Warehouse id ${organizationId} has no corresponding Organization Warehouse")
        }
        val orgWh: OrganizationWarehouse = maybeOrganizationWarehouse!!

        val maybeWarehouseJdbc = warehouses.get(orgWh.warehouseKey)
        requireNotNull(maybeWarehouseJdbc) {
            throw Exception("Warehouse id ${orgWh.organizationWarehouseId} has no corresponding system warehouse")
        }
        val warehouseJdbc: JdbcConnectionParameters = maybeWarehouseJdbc!!

        val jdbcUrl = appendDatabaseToJdbcPartial(warehouseJdbc.url, orgWh.name) + if (useSsl) {
            "?sslmode=require"
        } else {
            ""
        }
        val config = HikariConfig()
        config.setJdbcUrl(jdbcUrl)
        config.setUsername( warehouseJdbc.username )
        config.setPassword( warehouseJdbc.password )
        config.addDataSourceProperty( "maximumPoolSize" , "5" )
        config.addDataSourceProperty( "connectionTimeout" , "60000" )

        return HikariDataSource(config)
    }

    private fun cacheLoader(): CacheLoader<String, HikariDataSource> {
        return CacheLoader.from { organizationId ->
            createDataSource(
                UUID.fromString(organizationId!!),
                false
            )
        }
    }

    fun getDatabaseName(organizationId: UUID): String {
        val maybeOrg = organizationWarehouses.getValue(organizationId)

        requireNotNull(maybeOrg) {
            logger.error("Database {} does not exist in the organizationWarehouses mapstore", organizationId)
            "Database {} does not exist in the organizationWarehouses mapstore"
        }
        return maybeOrg.name
    }

    fun deleteOrganizationDatabase(organizationId: UUID) {
        organizationWarehouses.delete(organizationId)
    }

    fun connectAsSuperuser(warehouseId: UUID): HikariDataSource {
        val maybeWarehouseJdbc = warehouses.getValue(warehouseId)

        requireNotNull(maybeWarehouseJdbc) {
            throw Exception("Warehouse id ${warehouseId} has no corresponding system warehouse")
        }
        val warehouseJdbc: JdbcConnectionParameters = maybeWarehouseJdbc!!

        val config = HikariConfig()
        config.setJdbcUrl( warehouseJdbc.url + "dev" )
        config.setUsername( warehouseJdbc.username )
        config.setPassword( warehouseJdbc.password )
        config.addDataSourceProperty( "maximumPoolSize" , "5" )
        config.addDataSourceProperty( "connectionTimeout" , "60000" )

        return HikariDataSource(config)
    }

    fun connectToOrg(organizationId: UUID): HikariDataSource {
        return perDbCache.get(organizationId.toString())
    }

    fun connectToOrgGettingName(organizationId: UUID): Pair<HikariDataSource, String> {
        val orgName = getDatabaseName(organizationId)
        return perDbCache.get(organizationId.toString()) to orgName
    }

    fun createWhAndConnect(
        orgWh: OrganizationWarehouse,
        createDatabase: (orgWh: OrganizationWarehouse) -> Unit
    ): Pair<HikariDataSource, String> {
        createDatabase(orgWh)
        return perDbCache.get(orgWh.organizationId.toString()) to orgWh.name
    }

    fun appendDatabaseToJdbcPartial(jdbcStringNoDatabase: String, dbName: String): String {
        return "${jdbcStringNoDatabase.removeSuffix("/")}/$dbName"
    }

    fun getWarehouseType(warehouseId: UUID): String {
        return warehouses.getValue(warehouseId)._title
    }
}
