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

package com.openlattice.indexing.pods;

import com.geekbeast.rhizome.pods.ConfigurationLoader;
import com.openlattice.indexing.configuration.IndexerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.inject.Inject;
import java.io.IOException;

/**
 *
 */
@Configuration
public class IndexerConfigurationPod {
    private static final Logger logger = LoggerFactory.getLogger( IndexerConfigurationPod.class );

    @Inject
    private ConfigurationLoader configurationLoader;

    @Bean( name = "indexerConfiguration" )
    public IndexerConfiguration indexerConfiguration() throws IOException {
        IndexerConfiguration config = configurationLoader.load( IndexerConfiguration.class );
        logger.info( "Using {} indexer configuration: {}", configurationLoader.type(), config );
        return config;
    }
}
