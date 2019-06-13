package com.openlattice.twilio.pods

import com.amazonaws.services.s3.AmazonS3
import com.kryptnostic.rhizome.configuration.ConfigurationConstants
import com.kryptnostic.rhizome.configuration.amazon.AmazonLaunchConfiguration
import com.openlattice.ResourceConfigurationLoader
import com.openlattice.twilio.TwilioConfiguration
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile


private val logger = LoggerFactory.getLogger(TwilioConfigurationPod::class.java)

@Configuration
class TwilioConfigurationPod {
    @Autowired(required = false)
    private lateinit var awsS3: AmazonS3

    @Autowired(required = false)
    private lateinit var awsLaunchConfig: AmazonLaunchConfiguration

    @Bean(name = ["twilioConfiguration"])
    @Profile(ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE)
    fun localTwilioConfiguration(): TwilioConfiguration {
        val config = ResourceConfigurationLoader.loadConfiguration(TwilioConfiguration::class.java)
        logger.info("Using local aws datastore configuration: {}", config)
        return config
    }

    @Bean(name = ["twilioConfiguration"])
    @Profile(
            ConfigurationConstants.Profiles.AWS_CONFIGURATION_PROFILE,
            ConfigurationConstants.Profiles.AWS_TESTING_PROFILE
    )
    fun awsTwilioConfiguration() : TwilioConfiguration {
        val config = ResourceConfigurationLoader.loadConfigurationFromS3(
                awsS3!!,
                awsLaunchConfig!!.bucket,
                awsLaunchConfig.folder,
                TwilioConfiguration::class.java
        )
        logger.info("Using aws datastore configuration: {}", config)
        return config
    }
}