package com.openlattice.test

import com.hazelcast.core.HazelcastInstance
import com.openlattice.data.storage.DataSourceResolver
import com.openlattice.jdbc.DataSourceManager
import com.openlattice.postgres.PostgresTable
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.springframework.context.annotation.Bean
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@SuppressFBWarnings(value= ["RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE"], justification="Kotlin class parsing bug in spotbugs")
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