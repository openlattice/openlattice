

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
 */

package com.openlattice.neuron.audit;

import com.openlattice.neuron.signals.AuditableSignal;
import com.google.common.collect.ImmutableList;
import com.openlattice.neuron.signals.AuditableSignal;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumn;
import com.openlattice.postgres.PostgresQuery;
import com.openlattice.postgres.PostgresTable;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class AuditLogQueryService {
    private static final Logger logger = LoggerFactory.getLogger( AuditLogQueryService.class );
    private final HikariDataSource hds;

    private final String storeSql;

    // TODO: what would CassandraConfiguration be needed for in the future?
    public AuditLogQueryService( HikariDataSource hds ) {
        this.hds = hds;

        // Table
        String AUDIT_LOG = PostgresTable.AUDIT_LOG.getName();

        // Columns
        String ACL_KEY = PostgresColumn.ACL_KEY.getName();
        String EVENT_TYPE = PostgresColumn.EVENT_TYPE.getName();
        String PRINCIPAL_TYPE = PostgresColumn.PRINCIPAL_TYPE.getName();
        String PRINCIPAL_ID = PostgresColumn.PRINCIPAL_ID.getName();
        String TIME_UUID = PostgresColumn.TIME_UUID.getName();
        String AUDIT_ID = PostgresColumn.AUDIT_ID.getName();
        String DATA_ID = PostgresColumn.DATA_ID.getName();
        String BLOCK_ID = PostgresColumn.BLOCK_ID.getName();

        this.storeSql = PostgresQuery.insertRow( AUDIT_LOG,
                ImmutableList.of( ACL_KEY,
                        EVENT_TYPE,
                        PRINCIPAL_TYPE,
                        PRINCIPAL_ID,
                        TIME_UUID,
                        AUDIT_ID,
                        DATA_ID,
                        BLOCK_ID ) );
    }

    public void store( AuditableSignal signal ) {
        try ( Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( storeSql ) ) {
            ps.setArray( 1, PostgresArrays.createUuidArray( connection, signal.getAclKey().stream() ) );
            ps.setString( 2, signal.getType().name() );
            ps.setString( 3, signal.getPrincipal().getType().name() );
            ps.setString( 4, signal.getPrincipal().getId() );
            ps.setObject( 5, signal.getTimeId() );
            ps.setObject( 6, signal.getAuditId() );
            ps.setObject( 7, signal.getDataId() );
            ps.setObject( 8, signal.getBlockId() );
            ps.execute();
            connection.close();
        } catch ( SQLException e ) {
            logger.debug( "Unable to store signal", e );
        }
    }
}
