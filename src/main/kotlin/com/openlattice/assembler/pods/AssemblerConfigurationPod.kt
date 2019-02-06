/*
 * Copyright (C) 2019. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.assembler.pods

import com.amazonaws.services.s3.AmazonS3
import com.kryptnostic.rhizome.configuration.ConfigurationConstants
import com.kryptnostic.rhizome.configuration.amazon.AmazonLaunchConfiguration
import com.openlattice.ResourceConfigurationLoader
import com.openlattice.assembler.AssemblerConfiguration
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

private val logger = LoggerFactory.getLogger(AssemblerConfigurationPod::class.java)

/**
 *
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
class AssemblerConfigurationPod {
    @Autowired(required = false)
    private lateinit var awsS3: AmazonS3

    @Autowired(required = false)
    private lateinit var awsLaunchConfig: AmazonLaunchConfiguration

    @Bean(name = ["assemblerConfiguration"])
    @Profile(ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE)
    fun localAssemblerConfiguration(): AssemblerConfiguration {
        val config = ResourceConfigurationLoader.loadConfiguration(AssemblerConfiguration::class.java)
        logger.info("Using local aws datastore configuration: {}", config)
        return config
    }

    @Bean(name = ["assemblerConfiguration"])
    @Profile(
            ConfigurationConstants.Profiles.AWS_CONFIGURATION_PROFILE,
            ConfigurationConstants.Profiles.AWS_TESTING_PROFILE
    )
    fun awsAssemblerConfiguration() : AssemblerConfiguration {
        val config = ResourceConfigurationLoader.loadConfigurationFromS3(
                awsS3!!,
                awsLaunchConfig!!.bucket,
                awsLaunchConfig.folder,
                AssemblerConfiguration::class.java
        )
        logger.info("Using aws datastore configuration: {}", config)
        return config
    }
}
