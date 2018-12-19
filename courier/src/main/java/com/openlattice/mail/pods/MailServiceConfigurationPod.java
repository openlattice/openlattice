/*
 * Copyright (C) 2018. OpenLattice, Inc.
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

package com.openlattice.mail.pods;

import com.amazonaws.services.s3.AmazonS3;
import com.kryptnostic.rhizome.configuration.ConfigurationConstants.Profiles;
import com.kryptnostic.rhizome.configuration.amazon.AmazonLaunchConfiguration;
import com.openlattice.ResourceConfigurationLoader;
import com.openlattice.mail.config.MailServiceConfig;
import java.io.IOException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Configuration class for loading configuration information for mail service pod.
 */
@Configuration
public class MailServiceConfigurationPod {
    @Autowired( required = false )
    private AmazonS3 awsS3;

    @Autowired( required = false )
    private AmazonLaunchConfiguration awsLaunchConfig;

    @Bean
    @Profile( Profiles.LOCAL_CONFIGURATION_PROFILE )
    public MailServiceConfig mailServiceConfig() throws IOException {
        return ResourceConfigurationLoader.loadConfiguration( MailServiceConfig.class );
    }

    @Bean
    @Profile( Profiles.AWS_CONFIGURATION_PROFILE )
    public MailServiceConfig awsMailServiceConfig() {
        return ResourceConfigurationLoader.loadConfigurationFromS3( awsS3,
                awsLaunchConfig.getBucket(),
                awsLaunchConfig.getFolder(),
                MailServiceConfig.class );
    }
}
