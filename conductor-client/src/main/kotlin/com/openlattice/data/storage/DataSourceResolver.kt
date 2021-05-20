package com.openlattice.data.storage

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.hazelcast.core.HazelcastInstance
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.jdbc.DataSourceManager
import com.zaxxer.hikari.HikariDataSource
import java.util.*
import java.util.concurrent.TimeUnit

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class DataSourceResolver(hazelcastInstance: HazelcastInstance, val dataSourceManager: DataSourceManager) {
    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)
    private val resolverCache = CacheBuilder
            .newBuilder()
            .concurrencyLevel(Runtime.getRuntime().availableProcessors() - 1)
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .maximumSize(8192)
            .build(CacheLoader.from { entitySetId: UUID? ->
                entitySets.getValue(entitySetId).datastore //TODO: Replace with an entry processor read
            })

    fun resolve(entitySetId: UUID): HikariDataSource = dataSourceManager.getDataSource(resolverCache.get(entitySetId))
}