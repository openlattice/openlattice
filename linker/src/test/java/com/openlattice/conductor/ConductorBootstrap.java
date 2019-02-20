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
import com.openlattice.indexing.Linker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class ConductorBootstrap {
    protected static final Linker LINKER;

    static {
        LINKER = new Linker();
        final var logger = LoggerFactory.getLogger( ConductorBootstrap.class );
        if ( NetworkUtils.isRunningOnHost( "bamboo.openlattice.com" ) ) {
            LoggerFactory.getLogger( ConductorBootstrap.class ).info( "Running on bamboo!" );
            try {
                LINKER.start( "awstest", "postgres", "keras" );
            } catch ( Exception e ) {
                logger.error( "Unable to bootstrap condcutor with profiles: {}",
                        LINKER.getContext().getEnvironment().getActiveProfiles() );
                throw new IllegalStateException( "Unable to to boostrap conductor");
            }
        } else {
            LoggerFactory.getLogger( ConductorBootstrap.class ).info( "Not running on bamboo!" );
            try {
                LINKER.start( "local", "postgres", "keras" );
            } catch ( Exception e ) {
                logger.error( "Unable to bootstrap condcutor with profiles: {}",
                        LINKER.getContext().getEnvironment().getActiveProfiles() );
                throw new IllegalStateException( "Unable to to boostrap conductor");
            }
        }
    }

    protected final Logger logger = LoggerFactory.getLogger( getClass() );

}
