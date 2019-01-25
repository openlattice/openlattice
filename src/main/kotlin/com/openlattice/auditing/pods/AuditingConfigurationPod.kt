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

package com.openlattice.auditing.pods

import com.amazonaws.services.s3.AmazonS3
import com.kryptnostic.rhizome.configuration.ConfigurationConstants
import com.kryptnostic.rhizome.configuration.amazon.AmazonLaunchConfiguration
import com.openlattice.ResourceConfigurationLoader
import com.openlattice.auditing.AuditingConfiguration
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer
import com.openlattice.hazelcast.serializers.FullQualifiedNameStreamSerializer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

private val logger = LoggerFactory.getLogger(AuditingConfigurationPod::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
class AuditingConfigurationPod {

    init {
        FullQualifiedNameJacksonSerializer.registerWithMapper(ResourceConfigurationLoader.getYamlMapper())
    }


    @Autowired(required = false)
    private val awsS3: AmazonS3? = null

    @Autowired(required = false)
    private val awsLaunchConfig: AmazonLaunchConfiguration? = null

    @Bean(name = ["auditingConfiguration"])
    @Profile(ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE)
    fun getLocalAuditingConfiguration(): AuditingConfiguration {
        val config = ResourceConfigurationLoader.loadConfiguration(AuditingConfiguration::class.java)
        logger.info("Using local aws auditing configuration: {}", config)
        return config
    }

    @Bean(name = ["auditingConfiguration"])
    @Profile(
            ConfigurationConstants.Profiles.AWS_CONFIGURATION_PROFILE,
            ConfigurationConstants.Profiles.AWS_TESTING_PROFILE
    )
    fun getAwsAuditingConfiguration(): AuditingConfiguration {
        val config = ResourceConfigurationLoader.loadConfigurationFromS3(
                awsS3!!,
                awsLaunchConfig!!.bucket,
                awsLaunchConfig.folder,
                AuditingConfiguration::class.java
        )
        logger.info("Using aws auditing configuration: {}", config)
        return config
    }
}