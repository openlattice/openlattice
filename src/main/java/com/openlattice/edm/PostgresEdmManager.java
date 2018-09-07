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

import static com.openlattice.postgres.DataTables.propertyTableName;
import static com.openlattice.postgres.DataTables.quote;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.eventbus.Subscribe;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principal;
import com.openlattice.data.PropertyUsageSummary;
import com.openlattice.edm.events.EntitySetCreatedEvent;
import com.openlattice.edm.events.PropertyTypeCreatedEvent;
import com.openlattice.edm.events.PropertyTypeFqnChangedEvent;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.postgres.*;
import com.openlattice.postgres.streams.PostgresIterable;
import com.openlattice.postgres.streams.StatementHolder;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PostgresEdmManager implements DbEdmManager {
    private static final Logger logger = LoggerFactory.getLogger( PostgresEdmManager.class );

    private final PostgresTableManager ptm;
    private final HikariDataSource     hds;

    private final String getAllEntitySets;
    private final String getEntitySet;
    private final String getEntitySetsByType;
    private final String getAllPropertyTypeIds;
    private final String getPropertyTypeSummary;

    public PostgresEdmManager( HikariDataSource hds ) {
        this.ptm = new PostgresTableManager( hds );
        this.hds = hds;

        // Tables
        String ENTITY_SETS = PostgresTable.ENTITY_SETS.getName();

        // Properties
        String NAME = PostgresColumn.NAME.getName();
        String ENTITY_TYPE_ID = PostgresColumn.ENTITY_TYPE_ID.getName();
        String ENTITY_SET_ID = PostgresColumn.ENTITY_SET_ID.getName();

        // SQL queries
        this.getAllEntitySets = "SELECT * FROM ".concat( ENTITY_SETS ).concat( ";" );
        this.getEntitySet = "SELECT * FROM ".concat( ENTITY_SETS ).concat( " WHERE " ).concat( NAME )
                .concat( " = ?;" );
        this.getEntitySetsByType = "SELECT * FROM ".concat( ENTITY_SETS ).concat( " WHERE " ).concat( ENTITY_TYPE_ID )
                .concat( " = ?;" );
        this.getAllPropertyTypeIds = "SELECT id from \"property_types\";"; //fix later
        this.getPropertyTypeSummary = "SELECT entity_type_id, entity_set_id, count(*) FROM $propertyTableName LEFT JOIN entity_sets on entity_set_id = entity_sets.id " +
                "GROUP BY (entity_type_id,entity_set_id);";
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
            tables.add( propertyTableName( pt.getId() ) );
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

    private String renameColumn( String table, String current, String update ) {
        return String.format( "ALTER TABLE %s RENAME COLUMN %s TO %s", table, current, update );
    }

    public EntitySet getEntitySet( String entitySetName ) {
        EntitySet entitySet = null;
        try ( Connection connection = hds.getConnection();
              PreparedStatement ps = connection.prepareStatement( getEntitySet ) ) {
            ps.setString( 1, entitySetName );
            ResultSet rs = ps.executeQuery();
            if ( rs.next() ) {
                entitySet = ResultSetAdapters.entitySet( rs );
            }
            rs.close();
        } catch ( SQLException e ) {
            logger.error( "Unable to load entity set {}", entitySetName, e );
        }
        return entitySet;
    }

    public Iterable<EntitySet> getAllEntitySets() {
        try ( Connection connection = hds.getConnection();
              PreparedStatement ps = connection.prepareStatement( getAllEntitySets );
              ResultSet rs = ps.executeQuery() ) {
            List<EntitySet> result = Lists.newArrayList();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.entitySet( rs ) );
            }
            return result;
        } catch ( SQLException e ) {
            logger.error( "Unable to load all entity sets", e );
            return ImmutableList.of();
        }
    }

    public Iterable<EntitySet> getAllEntitySetsForType( UUID entityTypeId ) {
        try ( Connection connection = hds.getConnection();
              PreparedStatement ps = connection.prepareStatement( getEntitySetsByType ) ) {
            List<EntitySet> result = Lists.newArrayList();
            ps.setObject( 1, entityTypeId );
            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.entitySet( rs ) );
            }

            connection.close();
            return result;
        } catch ( SQLException e ) {
            logger.debug( "Unable to load entity sets for entity type id {}", entityTypeId.toString(), e );
            return ImmutableList.of();
        }
    }

    public Set<UUID> getAllPropertyTypeIds() {
        try ( Connection connection = hds.getConnection();
              PreparedStatement ps = connection.prepareStatement( getAllPropertyTypeIds ) ) {
            Set<UUID> result = Sets.newHashSet();
            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.id( rs ) );
            }

            connection.close();
            return result;
        } catch ( SQLException e ) {
            logger.error( "Unable to load property type ids", e );
            throw new IllegalStateException( "Unable to load property type ids.", e );
        }
    }

    public Iterable<PropertyUsageSummary> getPropertyUsageSummary(String propertyTableName ) {
        return new PostgresIterable<>( () -> {
            try {
                Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( getPropertyTypeSummary.replace("$propertyTableName", propertyTableName) );
                ResultSet rs = ps.executeQuery();
                return new StatementHolder( connection, ps, rs );
            } catch ( SQLException e ) {
                logger.error( "Unable to create statement holder!", e );
                throw new IllegalStateException( "Unable to create statement holder.", e );
            }
        }, rs -> {
            try {
                return ResultSetAdapters.propertyUsageSummary( rs );
            } catch ( SQLException e ) {
                logger.error( "Unable to load property summary information.", e );
                throw new IllegalStateException( "Unable to load property summary information.", e );
            }
        } );
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
        PostgresTableDefinition ptd = DataTables.buildPropertyTableDefinition( propertyType );
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
    public void handlePropertyTypeFqnChanged( PropertyTypeFqnChangedEvent fqnChangedEvent ) {
        try ( final Connection conn = hds.getConnection(); final Statement s = conn.createStatement() ) {
            s.execute( renameColumn(
                    quote( propertyTableName( fqnChangedEvent.getPropertyTypeId() ) ),
                    quote( fqnChangedEvent.getCurrent().getFullQualifiedNameAsString() ),
                    quote( fqnChangedEvent.getUpdate().getFullQualifiedNameAsString() ) ) );
        } catch ( SQLException e ) {
            logger.error( "Unable to process property type update.", e );

        }
    }

    @Subscribe
    @ExceptionMetered
    @Timed
    public void handlePropertyTypeCreated( PropertyTypeCreatedEvent propertyTypeCreatedEvent ) {
        try ( final Connection conn = hds.getConnection(); final Statement s = conn.createStatement() ) {
            PostgresTableDefinition ptd = DataTables
                    .buildPropertyTableDefinition( propertyTypeCreatedEvent.getPropertyType() );
            ptm.registerTables( ptd );
        } catch ( SQLException e ) {
            logger.error( "Unable to process property type creation.", e );
        }
    }

}
