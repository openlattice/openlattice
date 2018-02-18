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

package com.openlattice.datastore.scripts;

import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principal;
import com.google.common.collect.ImmutableList;
import com.openlattice.postgres.*;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

public class EmptyPermissionRemover {
    private       HikariDataSource hds;
    private final String           allPermissionsSql;
    private final String           deleteRowSql;

    public EmptyPermissionRemover( HikariDataSource hds ) {
        this.hds = hds;

        // Tables
        String PERMISSIONS = PostgresTable.PERMISSIONS.getName();

        // Columns
        String ACL_KEY = PostgresColumn.ACL_KEY.getName();
        String PRINCIPAL_TYPE = PostgresColumn.PRINCIPAL_TYPE.getName();
        String PRINCIPAL_ID = PostgresColumn.PRINCIPAL_ID.getName();

        this.allPermissionsSql = PostgresQuery.selectFrom( PERMISSIONS ).concat( PostgresQuery.END );

        this.deleteRowSql = PostgresQuery.deleteFrom( PERMISSIONS )
                .concat( PostgresQuery.whereEq( ImmutableList.of( ACL_KEY, PRINCIPAL_TYPE, PRINCIPAL_ID ), true ) );

    }

    public void run() {
        try ( Connection connection = hds.getConnection() ) {
            ResultSet rs = connection.prepareStatement( allPermissionsSql ).executeQuery();
            while ( rs.next() ) {
                EnumSet<Permission> permissions = ResultSetAdapters.permissions( rs );
                if ( permissions == null || permissions.isEmpty() ) {
                    List<UUID> aclKey = ResultSetAdapters.aclKey( rs );
                    Principal principal = ResultSetAdapters.principal( rs );
                    PreparedStatement ps = connection.prepareStatement( deleteRowSql );
                    ps.setArray( 1, PostgresArrays.createUuidArray( connection, aclKey.stream() ) );
                    ps.setString( 2, principal.getType().name() );
                    ps.setString( 3, principal.getId() );
                    ps.execute();
                }
            }
            connection.close();
        } catch ( SQLException e ) {}
    }
}
