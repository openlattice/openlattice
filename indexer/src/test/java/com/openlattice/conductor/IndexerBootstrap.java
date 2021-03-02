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

package com.openlattice.conductor;

import com.geekbeast.rhizome.NetworkUtils;
import com.kryptnostic.rhizome.configuration.ConfigurationConstants;
import com.openlattice.auditing.AuditingProfiles;
import com.openlattice.datastore.constants.DatastoreProfiles;
import com.openlattice.indexing.Indexer;
import com.openlattice.postgres.PostgresPod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class IndexerBootstrap {

    protected static final Indexer INDEXER;

    private static String[] localArgs = {
            ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE,
            PostgresPod.PROFILE,
            DatastoreProfiles.MEDIA_LOCAL_PROFILE,
            AuditingProfiles.LOCAL_AUDITING_PROFILE
    };
    private static String[] bambooArgs = {
            ConfigurationConstants.Profiles.AWS_TESTING_PROFILE,
            PostgresPod.PROFILE
    };

    static {
        final var logger = LoggerFactory.getLogger( IndexerBootstrap.class );
        INDEXER = new Indexer();
        String[] profiles;
        if ( NetworkUtils.isRunningOnHost( "bamboo.openlattice.com" ) ) {
            logger.info( "Running on bamboo!" );
            profiles = bambooArgs;
        } else {
            logger.info( "Not running on bamboo!" );
            profiles = localArgs;
        }
        try {
            INDEXER.start( profiles );
        } catch ( Exception e ) {
            logger.error( "Unable to bootstrap conductor with profiles: " + Arrays.toString( profiles ) );
        }
    }

    protected final Logger logger = LoggerFactory.getLogger( getClass() );

}
