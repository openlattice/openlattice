package com.openlattice.twilio.pods

import com.kryptnostic.rhizome.pods.ConfigurationLoader
import com.openlattice.twilio.TwilioConfiguration
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.inject.Inject


private val logger = LoggerFactory.getLogger(TwilioConfigurationPod::class.java)

@Configuration
class TwilioConfigurationPod {
    @Inject
    private lateinit var configurationLoader: ConfigurationLoader

    @Bean
    fun twilioConfiguration(): TwilioConfiguration {
        return configurationLoader.logAndLoad( "twilio", TwilioConfiguration::class.java )
    }
}