package com.openlattice.organizations.pods

import com.kryptnostic.rhizome.pods.ConfigurationLoader
import com.openlattice.organizations.OrganizationExternalDatabaseConfiguration
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.inject.Inject

private val logger = LoggerFactory.getLogger(OrganizationExternalDatabaseConfigurationPod::class.java)

@Configuration
class OrganizationExternalDatabaseConfigurationPod {
    @Inject
    private lateinit var configurationLoader: ConfigurationLoader

    @Bean
    fun organizationExternalDatabaseConfiguration(): OrganizationExternalDatabaseConfiguration {
        return configurationLoader.logAndLoad( "organization external database", OrganizationExternalDatabaseConfiguration::class.java )
    }
}