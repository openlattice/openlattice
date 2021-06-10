package com.openlattice.hazelcast.pods

import com.hazelcast.core.HazelcastInstance
import com.openlattice.data.storage.DataSourceResolver
import com.openlattice.jdbc.DataSourceManager
import com.openlattice.postgres.PostgresTable
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
class DataSourceResolverPodForTests {
    @Inject
    private lateinit var dataSourceManager: DataSourceManager

    @Inject
    private lateinit var hazelcastInstance: HazelcastInstance

    @Bean
    fun resolver(): DataSourceResolver {
        dataSourceManager.registerTablesWithAllDatasources(PostgresTable.E)
        dataSourceManager.registerTablesWithAllDatasources(PostgresTable.DATA)
        return DataSourceResolver(hazelcastInstance, dataSourceManager, true)
    }
}