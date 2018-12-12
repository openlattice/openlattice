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

package com.openlattice.datastore.pods;

import static com.openlattice.authorization.AuthorizingComponent.logger;

import com.amazonaws.services.s3.AmazonS3;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.kryptnostic.rhizome.configuration.ConfigurationConstants.Profiles;
import com.kryptnostic.rhizome.configuration.amazon.AmazonLaunchConfiguration;
import com.openlattice.ResourceConfigurationLoader;
import com.openlattice.data.storage.AwsBlobDataService;
import com.openlattice.data.storage.ByteBlobDataManager;
import com.openlattice.data.storage.LocalBlobDataService;
import com.openlattice.datastore.configuration.DatastoreConfiguration;
import com.openlattice.datastore.constants.DatastoreProfiles;
import com.zaxxer.hikari.HikariDataSource;
import javax.inject.Inject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Profile;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
public class ByteBlobServicePod {
    @Inject
    private HikariDataSource hikariDataSource;

    @Inject
    private ListeningExecutorService executorService;

    @Autowired( required = false )
    private AmazonS3 awsS3;

    @Autowired( required = false )
    private AmazonLaunchConfiguration awsLaunchConfig;

    @Bean( name = "datastoreConfiguration" )
    @Profile( { Profiles.LOCAL_CONFIGURATION_PROFILE } )
    public DatastoreConfiguration getLocalAwsDatastoreConfiguration() {
        DatastoreConfiguration config = ResourceConfigurationLoader.loadConfiguration( DatastoreConfiguration.class );
        logger.info( "Using local aws datastore configuration: {}", config );
        return config;
    }

    @Bean( name = "datastoreConfiguration" )
    @Profile( { Profiles.AWS_CONFIGURATION_PROFILE, Profiles.AWS_TESTING_PROFILE } )
    public DatastoreConfiguration getAwsDatastoreConfiguration() {
        DatastoreConfiguration config = ResourceConfigurationLoader.loadConfigurationFromS3( awsS3,
                awsLaunchConfig.getBucket(),
                awsLaunchConfig.getFolder(),
                DatastoreConfiguration.class );
        logger.info( "Using aws datastore configuration: {}", config );
        return config;
    }

    @Bean( name = "byteBlobDataManager" )
    @DependsOn( "datastoreConfiguration" )
    @Profile( { DatastoreProfiles.MEDIA_LOCAL_PROFILE } )
    public ByteBlobDataManager localBlobDataManager() {
        return new LocalBlobDataService( hikariDataSource );
    }

    @Bean( name = "byteBlobDataManager" )
    @DependsOn( "datastoreConfiguration" )
    @Profile( { DatastoreProfiles.MEDIA_LOCAL_AWS_PROFILE } )
    public ByteBlobDataManager localAwsBlobDataManager() {
        return new AwsBlobDataService( getLocalAwsDatastoreConfiguration(), executorService );
    }

    @Bean( name = "byteBlobDataManager" )
    @DependsOn( "datastoreConfiguration" )
    @Profile( { Profiles.AWS_CONFIGURATION_PROFILE, Profiles.AWS_TESTING_PROFILE } )
    public ByteBlobDataManager awsBlobDataManager() {
        return new AwsBlobDataService( getAwsDatastoreConfiguration(), executorService );
    }

}
