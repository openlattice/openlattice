package com.openlattice.postgres.pods

import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.transporter.pods.TransporterPod
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
    private lateinit var organizationService: HazelcastOrganizationService

    @Bean
    fun externalDatabaseConnectionManager(): ExternalDatabaseConnectionManager {
        LoggerFactory.getLogger(TransporterPod::class.java).info("Constructing ExternalDatabaseConnectionManager")
        return ExternalDatabaseConnectionManager(assemblerConfiguration, organizationService)
    }
}