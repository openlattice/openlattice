package com.openlattice.postgres.pods

import com.hazelcast.core.HazelcastInstance
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.postgres.external.ExternalWarehouseConnectionManager
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.inject.Inject

/**
 *
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@Configuration
class ExternalDatabaseConnectionManagerPod {
    @Inject
    private lateinit var assemblerConfiguration: AssemblerConfiguration

    @Inject
    private lateinit var hazelcastInstance: HazelcastInstance

    @Bean
    fun externalDatabaseConnectionManager(): ExternalDatabaseConnectionManager {
        LoggerFactory.getLogger(ExternalDatabaseConnectionManagerPod::class.java).info("Constructing ExternalDatabaseConnectionManager")
        return ExternalDatabaseConnectionManager(assemblerConfiguration, hazelcastInstance)
    }

    @Bean
    fun externalWarehouseConnectionManager(): ExternalWarehouseConnectionManager {
        LoggerFactory.getLogger(ExternalWarehouseConnectionManager::class.java).info("Constructing ExternalWarehouseConnectionManager")
        return ExternalWarehouseConnectionManager(hazelcastInstance)
    }
}
