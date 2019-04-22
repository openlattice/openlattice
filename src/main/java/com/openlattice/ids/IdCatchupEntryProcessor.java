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

package com.openlattice.ids;

import static com.google.common.base.Preconditions.checkNotNull;

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;
import com.openlattice.postgres.PostgresColumn;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map.Entry;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class IdCatchupEntryProcessor extends AbstractRhizomeEntryProcessor<Integer, Range, Void> {
    private static final Logger logger = LoggerFactory.getLogger( IdCatchupEntryProcessor.class );

    private final HikariDataSource hds;

    public IdCatchupEntryProcessor( HikariDataSource hds ) {
        this.hds = checkNotNull( hds );
    }

    @Override
    public Void process( Entry<Integer, Range> entry ) {
        final Range range = checkNotNull( entry.getValue() ); //Range should never be null in the EP.
        int counter = 0;
        try ( final Connection connection = hds.getConnection(); final var ps = prepareExistQuery( connection ) ) {

            while ( exists( ps, range.peek() ) ) {
                range.nextId();
                counter++;
                if ( ( counter % 10000 ) == 0 ) {
                    logger.info( "Incremented range with base {} by {}", range.getBase(), counter );
                }
            }

            entry.setValue( range );
            logger.info( "Caught up range with base {} by {} increments", range.getBase(), counter );
        } catch ( SQLException e ) {
            logger.error( "Error catching up ranges.", e );
        }
        return null;
    }

    public PreparedStatement prepareExistQuery( Connection connection ) throws SQLException {
        return connection.prepareStatement(
                "SELECT count(*) from " + PostgresTable.IDS.getName() +
                        " WHERE " + PostgresColumn.ID_VALUE.getName() + " = ?" );
    }

    public boolean exists( PreparedStatement ps, UUID id ) throws SQLException {
        ps.setObject( 1, id );
        //Count query always guaranteed to have one row.
        final var rs = ps.executeQuery();
        rs.next();
        return ResultSetAdapters.count( rs ) > 0;
    }
}

