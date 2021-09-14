package com.openlattice.data.storage

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.hazelcast.core.HazelcastInstance
import com.openlattice.IdConstants
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.jdbc.DataSourceManager
import com.zaxxer.hikari.HikariDataSource
import net.snowflake.client.jdbc.internal.apache.arrow.flatbuf.Bool
import java.util.*
import java.util.concurrent.TimeUnit

/**
 * This class has to be open for mocking.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
open class DataSourceResolver @JvmOverloads constructor(
    hazelcastInstance: HazelcastInstance,
    val dataSourceManager: DataSourceManager,
    val defaultOnMissingEntitySet: Boolean = false
) {
    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)
    private val resolverCache = CacheBuilder
        .newBuilder()
        .concurrencyLevel(Runtime.getRuntime().availableProcessors() - 1)
        .expireAfterAccess(5, TimeUnit.MINUTES)
        .maximumSize(8192)
        .build(if (defaultOnMissingEntitySet) {
            CacheLoader.from { entitySetId: UUID? ->
                entitySets[entitySetId]?.datastore ?: DataSourceManager.DEFAULT_DATASOURCE
            }
        } else {
            CacheLoader.from { entitySetId: UUID? ->
                entitySets.getValue(entitySetId).datastore //TODO: Replace with an entry processor read
            }
        })

    fun resolve(entitySetId: UUID): HikariDataSource = dataSourceManager.getDataSource(getDataSourceName(entitySetId))
    fun getDataSourceName(entitySetId: UUID): String = if (entitySetId == IdConstants.LINKING_ENTITY_SET_ID.id) {
        DataSourceManager.DEFAULT_DATASOURCE
    } else resolverCache.get(entitySetId)

    fun getDataSource(dataSourceName: String): HikariDataSource = dataSourceManager.getDataSource(dataSourceName)
    fun getDefaultDataSource(): HikariDataSource = dataSourceManager.getDefaultDataSource()
}