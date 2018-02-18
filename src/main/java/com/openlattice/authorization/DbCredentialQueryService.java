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

package com.openlattice.authorization;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DbCredentialQueryService {
    private static final Logger logger       = LoggerFactory.getLogger( DbCredentialQueryService.class );
    private static final String SET_PASSWORD = "ALTER USER \"%s\" WITH PASSWORD '%s'";
    private static final String DELETE_USER  = "DELETE USER \"%s\"";
    private static final String CREATE_USER  = "CREATE USER \"%s\" WITH PASSWORD '%s'";

    private final HikariDataSource hds;

    public DbCredentialQueryService( HikariDataSource hds ) {
        this.hds = hds;
    }

    public boolean createUser( String userId, String credential ) {
        logger.info( "About to create connection for userId {}", userId );
        try ( Connection conn = hds.getConnection(); Statement st = conn.createStatement() ) {
            String query = String.format( CREATE_USER, userId, credential );
            st.execute( query );
            return true;
        } catch ( Exception e ) {
            logger.error( "Unable to create user {}", userId, e );
        }
        logger.info( "Successfully executed statement for userId {}", userId );
        return false;
    }

    public boolean setCredential( String userId, String credential ) {
        try ( Connection conn = hds.getConnection(); Statement st = conn.createStatement() ) {
            st.execute( String.format( SET_PASSWORD, userId, credential ) );
            return true;
        } catch ( SQLException e ) {
            logger.error( "Unable to set creds for user {}", userId, e );
        }
        return false;
    }

    public void deleteCredential( String userId ) {
        try ( Connection conn = hds.getConnection(); Statement st = conn.createStatement() ) {
            st.execute( String.format( DELETE_USER, userId ) );
        } catch ( SQLException e ) {
            logger.error( "Unable to delete user {}", userId, e );
        }
    }

}
