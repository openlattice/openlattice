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

package com.openlattice.datastore.services;

import com.openlattice.data.Entity;
import com.openlattice.edm.EntitySet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.data.PropertySummary;
import com.openlattice.postgres.PostgresColumn;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.openlattice.postgres.streams.PostgresIterable;
import com.openlattice.postgres.streams.StatementHolder;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PostgresEntitySetManager {
    private static final Logger logger = LoggerFactory.getLogger( PostgresEntitySetManager.class );
    private final HikariDataSource hds;

    private final String getAllEntitySets;
    private final String getEntitySet;
    private final String getEntitySetsByType;
    //private final String getPropertyTypeSummary;

    public PostgresEntitySetManager( HikariDataSource hds ) {
        this.hds = hds;

        // Tables
        String ENTITY_SETS = PostgresTable.ENTITY_SETS.getName();
        //String PROPERTY_TYPES = PostgresTable.PROPERTY_TYPES.getName(); <- not sure if I use this here or not

        // Properties
        String NAME = PostgresColumn.NAME.getName();
        String ENTITY_TYPE_ID = PostgresColumn.ENTITY_TYPE_ID.getName();

        // SQL queries
        this.getAllEntitySets = "SELECT * FROM ".concat( ENTITY_SETS ).concat( ";" );
        this.getEntitySet = "SELECT * FROM ".concat( ENTITY_SETS ).concat( " WHERE " ).concat( NAME )
                .concat( " = ?;" );
        this.getEntitySetsByType = "SELECT * FROM ".concat( ENTITY_SETS ).concat( " WHERE " ).concat( ENTITY_TYPE_ID )
                .concat( " = ?;" );
        /*
        this.getPropertyTypeSummary = "SELECT entity_type_id, entity_set_id, count(*) FROM "
                .concat( PROPERTY_TYPES ).concat(" LEFT JOIN entity_sets on entity_set_id = entity_sets.id " +
                        "GROUP BY (entity_type_id,entity_set_id);");
                        */
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
                ResultSet rs = ps.executeQuery()) {
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

    public Stream<PropertySummary> getPropertySummary( String propertyTableName) {
            return new PostgresIterable<>( () -> {
                final ResultSet rs;
                final Connection connection;
                final PreparedStatement statement;
                String propertySummaryQuery = "SELECT entity_type_id, entity_set_id, count(*) FROM "
                        .concat( propertyTableName ).concat(" LEFT JOIN entity_sets on entity_set_id = entity_sets.id " +
                                "GROUP BY (entity_type_id,entity_set_id);");
                try {
                    connection = hds.getConnection();
                    statement = connection.prepareStatement( propertySummaryQuery );
                    rs = statement.executeQuery();
                    return new StatementHolder( connection, statement, rs );
                } catch ( SQLException e ) {
                    logger.error( "Unable to create statement holder!", e );
                    throw new IllegalStateException( "Unable to create statement holder.", e );
                }
            }, rs -> {
                try {
                    return ResultSetAdapters.propertySummary( rs );
                } catch ( SQLException e ) {
                    logger.error( "Unable to load entity information.", e );
                    throw new IllegalStateException( "Unable to load entity information.", e );
                }
            } ).stream();
    }

}
