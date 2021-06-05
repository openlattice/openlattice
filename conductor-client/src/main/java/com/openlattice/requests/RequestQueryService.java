

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

package com.openlattice.requests;

import com.openlattice.authorization.AceKey;
import com.openlattice.authorization.Principal;
import com.dataloom.streams.StreamUtil;
import com.google.common.collect.Lists;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumn;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.openlattice.requests.RequestStatus;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class RequestQueryService {
    protected final Logger logger = LoggerFactory.getLogger( RequestQueryService.class );

    private final HikariDataSource hds;

    private final String getRequestKeysForPrincipalSql;
    private final String getRequestKeysForPrincipalAndStatusSql;
    private final String getRequestKeysForAclKeySql;
    private final String getRequestKeysForAclKeyAndStatusSql;

    public RequestQueryService( HikariDataSource hds ) {
        this.hds = hds;

        // Tables
        String REQUESTS = PostgresTable.REQUESTS.getName();

        // Columns
        String ACL_KEY = PostgresColumn.ACL_KEY.getName();
        String PRINCIPAL_TYPE = PostgresColumn.PRINCIPAL_TYPE.getName();
        String PRINCIPAL_ID = PostgresColumn.PRINCIPAL_ID.getName();
        String STATUS = PostgresColumn.STATUS.getName();

        getRequestKeysForPrincipalSql = "SELECT ".concat( ACL_KEY ).concat( " FROM " ).concat( REQUESTS )
                .concat( " WHERE " ).concat( PRINCIPAL_TYPE ).concat( " = ? AND " ).concat( PRINCIPAL_ID )
                .concat( " = ?;" );
        getRequestKeysForPrincipalAndStatusSql = "SELECT ".concat( ACL_KEY ).concat( " FROM " ).concat( REQUESTS )
                .concat( " WHERE " ).concat( PRINCIPAL_TYPE ).concat( " = ? AND " ).concat( PRINCIPAL_ID )
                .concat( " = ? AND " ).concat( STATUS ).concat( " = ?;" );
        getRequestKeysForAclKeySql = "SELECT ".concat( ACL_KEY ).concat( ", " ).concat( PRINCIPAL_TYPE ).concat( ", " )
                .concat( PRINCIPAL_ID ).concat( " FROM " ).concat( REQUESTS ).concat( " WHERE " ).concat( ACL_KEY )
                .concat( " = ?;" );
        getRequestKeysForAclKeyAndStatusSql = "SELECT ".concat( ACL_KEY ).concat( ", " ).concat( PRINCIPAL_TYPE )
                .concat( ", " )
                .concat( PRINCIPAL_ID ).concat( " FROM " ).concat( REQUESTS ).concat( " WHERE " ).concat( ACL_KEY )
                .concat( " = ? AND " ).concat( STATUS ).concat( " = ?;" );
    }

    public Stream<AceKey> getRequestKeys( Principal principal ) {
        try ( Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( getRequestKeysForPrincipalSql ) ) {
            List<AceKey> result = Lists.newArrayList();
            ps.setString( 1, principal.getType().name() );
            ps.setString( 2, principal.getId() );

            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( new AceKey( ResultSetAdapters.aclKey( rs ), principal ) );
            }
            connection.close();
            return StreamUtil.stream( result );
        } catch ( SQLException e ) {
            logger.debug( "Unable to get request keys.", e );
            return Stream.empty();
        }
    }

    public Stream<AceKey> getRequestKeys( Principal principal, RequestStatus requestStatus ) {
        try ( Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( getRequestKeysForPrincipalAndStatusSql ) ) {
            List<AceKey> result = Lists.newArrayList();
            ps.setString( 1, principal.getType().name() );
            ps.setString( 2, principal.getId() );
            ps.setString( 3, requestStatus.name() );

            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( new AceKey( ResultSetAdapters.aclKey( rs ), principal ) );
            }
            connection.close();
            return StreamUtil.stream( result );
        } catch ( SQLException e ) {
            logger.debug( "Unable to get request keys.", e );
            return Stream.empty();
        }
    }

    public Stream<AceKey> getRequestKeys( List<UUID> aclKey ) {
        try ( Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( getRequestKeysForAclKeySql ) ) {
            List<AceKey> result = Lists.newArrayList();
            ps.setArray( 1, PostgresArrays.createUuidArray( connection, aclKey.stream() ) );

            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.aceKey( rs ) );
            }
            connection.close();
            return StreamUtil.stream( result );
        } catch ( SQLException e ) {
            logger.debug( "Unable to get request keys.", e );
            return Stream.empty();
        }
    }

    public Stream<AceKey> getRequestKeys( List<UUID> aclKey, RequestStatus requestStatus ) {
        try ( Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( getRequestKeysForAclKeyAndStatusSql ) ) {
            List<AceKey> result = Lists.newArrayList();
            ps.setArray( 1, PostgresArrays.createUuidArray( connection, aclKey.stream() ) );
            ps.setString( 2, requestStatus.name() );

            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.aceKey( rs ) );
            }
            connection.close();
            return StreamUtil.stream( result );
        } catch ( SQLException e ) {
            logger.debug( "Unable to get request keys.", e );
            return Stream.empty();
        }
    }
}
