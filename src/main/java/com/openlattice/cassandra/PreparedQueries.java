/*
 * Copyright (C) 2017. OpenLattice, Inc
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
 */

package com.openlattice.cassandra;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.EnumMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PreparedQueries {
    private static final Logger                                   logger  = LoggerFactory
            .getLogger( PreparedQueries.class );
    private static final EnumMap<PreparedQuery, RegularStatement> queries = new EnumMap<>( PreparedQuery.class );
    private final Session                                   session;
    private final EnumMap<PreparedQuery, PreparedStatement> preparedQueries;

    public PreparedQueries( Session session ) {
        this.session = session;
        this.preparedQueries = new EnumMap<>( PreparedQuery.class );
        preparedQueries.putAll( Maps.transformValues( queries, session::prepare ) );
    }

    public PreparedStatement getPreparedQuery( PreparedQuery query ) {
        return Preconditions.checkNotNull( preparedQueries.get( query ), "Failed to retrieve prepared query." );
    }

    public static void registerPreparedQuery( PreparedQuery preparedQuery, RegularStatement stmt, Class<?> clazz ) {
        RegularStatement existingQuery = queries.putIfAbsent( preparedQuery, stmt );

        //        Preconditions.checkState( existingQuery != null , "Prepared statement already registered");
        if ( existingQuery != null ) {
            logger.warn( "{} query has already been registered by {}", preparedQuery.name(), clazz.getCanonicalName() );
        }
    }
}
