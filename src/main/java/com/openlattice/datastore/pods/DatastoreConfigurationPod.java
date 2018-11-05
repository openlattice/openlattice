package com.openlattice.datastore.pods;

import com.amazonaws.services.s3.AmazonS3;
import com.kryptnostic.rhizome.configuration.ConfigurationConstants;
import com.kryptnostic.rhizome.configuration.amazon.AmazonLaunchConfiguration;
import com.kryptnostic.rhizome.configuration.service.ConfigurationService;
import com.openlattice.ResourceConfigurationLoader;
import com.openlattice.datastore.configuration.DatastoreConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import javax.inject.Inject;
import java.io.IOException;

import static com.openlattice.authorization.AuthorizingComponent.logger;

@Configuration
public class DatastoreConfigurationPod {
    @Inject
    private ConfigurationService configurationService;

    @Autowired( required = false )
    private AmazonS3                  awsS3;
    @Autowired( required = false )
    private AmazonLaunchConfiguration awsLaunchConfig;

    @Bean( name = "datastoreConfiguration" )
    @Profile( ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE )
    public DatastoreConfiguration getLocalDatastoreConfiguration() throws IOException {
        DatastoreConfiguration config = configurationService.getConfiguration( DatastoreConfiguration.class );
        logger.info( "Using local datastore configuration: {}", config );
        return config;
    }

    @Bean( name = "datastoreConfiguration" )
    @Profile( { ConfigurationConstants.Profiles.AWS_CONFIGURATION_PROFILE, ConfigurationConstants.Profiles.AWS_TESTING_PROFILE } )
    public DatastoreConfiguration getAwsDatastoreConfiguration() throws IOException {
        DatastoreConfiguration config = ResourceConfigurationLoader.loadConfigurationFromS3( awsS3,
                awsLaunchConfig.getBucket(),
                awsLaunchConfig.getFolder(),
                DatastoreConfiguration.class );
        logger.info( "Using aws datastore configuration: {}", config );
        return config;
    }

}
