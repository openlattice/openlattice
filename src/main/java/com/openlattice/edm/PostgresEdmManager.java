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

package com.openlattice.edm;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.eventbus.Subscribe;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principal;
import com.openlattice.edm.events.EntitySetCreatedEvent;
import com.openlattice.edm.events.PropertyTypeCreatedEvent;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.postgres.DataTables;
import com.openlattice.postgres.PostgresTableDefinition;
import com.openlattice.postgres.PostgresTableManager;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PostgresEdmManager implements DbEdmManager {
    private static final Logger logger = LoggerFactory.getLogger( PostgresEdmManager.class );

    private final PostgresTableManager ptm;
    private final HikariDataSource     hds;

    public PostgresEdmManager( PostgresTableManager ptm, HikariDataSource hds ) {
        this.ptm = ptm;
        this.hds = hds;
    }

    @Override
    public void createEntitySet(
            EntitySet entitySet,
            Collection<PropertyType> propertyTypes ) throws SQLException {
        createEntitySetTable( entitySet );
        for ( PropertyType pt : propertyTypes ) {
            createPropertyTypeTableIfNotExist( entitySet, pt );
        }
        //Method is idempotent and should be re-executable in case of a failure.
    }

    @Override public void deleteEntitySet(
            EntitySet entitySet, Collection<PropertyType> propertyTypes ) {
        PostgresTableDefinition ptd = DataTables.buildEntitySetTableDefinition( entitySet );
        dropTable( ptd.getName() );
        removePropertiesFromEntitySet( entitySet, propertyTypes );

    }

    @Override public void removePropertiesFromEntitySet( EntitySet entitySet, PropertyType... propertyTypes ) {
        removePropertiesFromEntitySet( entitySet, Arrays.asList( propertyTypes ) );
    }

    @Override public void removePropertiesFromEntitySet(
            EntitySet entitySet, Collection<PropertyType> propertyTypes ) {
//        for ( PropertyType propertyType : propertyTypes ) {
//            PostgresTableDefinition ptd = DataTables.buildPropertyTableDefinition( entitySet, propertyType );
//            dropTable( ptd.getName() );
//        }
    }

    public void dropTable( String table ) {
        try ( Connection conn = hds.getConnection(); Statement s = conn.createStatement() ) {
            s.execute( "DROP TABLE " + table );
        } catch ( SQLException e ) {
            logger.error( "Encountered exception while dropping table: {}", table, e );
        }
    }

    @Override
    public void grant(
            Principal principal,
            EntitySet entitySet,
            Collection<PropertyType> propertyTypes,
            EnumSet<Permission> permissions ) {
        if ( permissions.isEmpty() ) {
            //I hate early returns but nesting will get too messy and this is pretty clear that granting
            //no permissions is a no-op.
            return;
        }

        List<String> tables = new ArrayList<>( propertyTypes.size() + 1 );
        tables.add( DataTables.entityTableName( entitySet.getId() ) );

        for ( PropertyType pt : propertyTypes ) {
            tables.add( DataTables.propertyTableName( pt.getId() ) );
        }

        String principalId = principal.getId();

        for ( String table : tables ) {
            for ( Permission p : permissions ) {
                String postgresPrivilege = DataTables.mapPermissionToPostgresPrivilege( p );
                String grantQuery = grantOnTable( table, principalId, postgresPrivilege );
                try ( Connection conn = hds.getConnection(); Statement s = conn.createStatement() ) {
                    s.execute( grantQuery );
                } catch ( SQLException e ) {
                    logger.error( "Unable to execute grant query {}", grantQuery, e );
                }
            }
        }
    }

    public void revoke(
            Principal principal,
            EntitySet entitySet,
            Collection<PropertyType> propertyTypes,
            EnumSet<Permission> permissions ) {

    }

    private String grantOnTable( String table, String principalId, String permission ) {
        return String.format( "GRANT %s ON TABLE %s TO %s", permission, table, principalId );
    }

    private void createEntitySetTable( EntitySet entitySet ) throws SQLException {
        PostgresTableDefinition ptd = DataTables.buildEntitySetTableDefinition( entitySet );
        ptm.registerTables( ptd );
    }

    /*
     * Quick note on this function. It is IfNotExists only because PostgresTableDefinition queries
     * all include an if not exists. If the behavior of that class changes this function should be updated
     * appropriately.
     */
    @VisibleForTesting
    public void createPropertyTypeTableIfNotExist( EntitySet entitySet, PropertyType propertyType )
            throws SQLException {
        PostgresTableDefinition ptd = DataTables.buildPropertyTableDefinition( entitySet, propertyType );
        ptm.registerTables( ptd );
    }

    @Subscribe
    @ExceptionMetered
    @Timed
    public void handleEntitySetCreated( EntitySetCreatedEvent entitySetCreatedEvent ) {
        try {
            createEntitySet( entitySetCreatedEvent.getEntitySet(), entitySetCreatedEvent.getPropertyTypes() );
        } catch ( SQLException e ) {
            logger.error( "Unable to create entity set {}", entitySetCreatedEvent.getEntitySet() );
        }
    }

    @Subscribe
    @ExceptionMetered
    @Timed
    public void handlePropertyTypeCreated(PropertyTypeCreatedEvent propertyTypeCreatedEvent ) {

    }

}
