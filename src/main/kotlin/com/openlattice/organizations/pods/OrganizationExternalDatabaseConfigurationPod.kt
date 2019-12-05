package com.openlattice.organizations.pods

import com.amazonaws.services.s3.AmazonS3
import com.kryptnostic.rhizome.configuration.ConfigurationConstants
import com.kryptnostic.rhizome.configuration.amazon.AmazonLaunchConfiguration
import com.openlattice.ResourceConfigurationLoader
import com.openlattice.organizations.OrganizationExternalDatabaseConfiguration
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

private val logger = LoggerFactory.getLogger(OrganizationExternalDatabaseConfigurationPod::class.java)

@Configuration
class OrganizationExternalDatabaseConfigurationPod {

    @Autowired(required = false)
    private lateinit var awsS3: AmazonS3

    @Autowired(required = false)
    private lateinit var awsLaunchConfig: AmazonLaunchConfiguration

    @Bean(name = ["organizationExternalDatabaseConfiguration"])
    @Profile(ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE)
    fun getLocalOrganizationExternalDatabaseConfiguration(): OrganizationExternalDatabaseConfiguration {
        val config = ResourceConfigurationLoader.loadConfiguration(OrganizationExternalDatabaseConfiguration::class.java)
        logger.info("Using local organization external database configuration: {}", config)
        return config
    }

    @Bean(name = ["organizationExternalDatabaseConfiguration"])
    @Profile(ConfigurationConstants.Profiles.AWS_CONFIGURATION_PROFILE,
            ConfigurationConstants.Profiles.AWS_TESTING_PROFILE
    )
    fun getAwsOrganizationExternalDatabaseConfiguration(): OrganizationExternalDatabaseConfiguration {
        val config = ResourceConfigurationLoader.loadConfigurationFromS3(
                awsS3!!,
                awsLaunchConfig!!.bucket,
                awsLaunchConfig.folder,
                OrganizationExternalDatabaseConfiguration::class.java
        )
        logger.info("Using aws organization external database configuration: {}", config)
        return config
    }

}