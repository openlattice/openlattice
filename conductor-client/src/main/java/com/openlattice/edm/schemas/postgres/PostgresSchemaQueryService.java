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

package com.openlattice.edm.schemas.postgres;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.openlattice.edm.schemas.SchemaQueryService;
import com.openlattice.postgres.PostgresColumn;
import com.openlattice.postgres.PostgresQuery;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresSchemaQueryService implements SchemaQueryService {
    protected final Logger logger = LoggerFactory.getLogger( PostgresSchemaQueryService.class );
    private final HikariDataSource hds;

    private final String propertyTypesInSchemaQuery;
    private final String entityTypesInSchemaQuery;
    private final String getNamespaces;

    public PostgresSchemaQueryService( HikariDataSource hds ) {
        this.hds = hds;

        // Tables
        String SCHEMAS = PostgresTable.SCHEMA.getName();
        String ENTITY_TYPES = PostgresTable.ENTITY_TYPES.getName();
        String PROPERTY_TYPES = PostgresTable.PROPERTY_TYPES.getName();

        // Columns
        String SCHEMA_LIST = PostgresColumn.SCHEMAS.getName();
        String NAMESPACE = PostgresColumn.NAMESPACE.getName();
        String ID = PostgresColumn.ID.getName();

        this.getNamespaces = PostgresQuery.selectDistinctFrom( SCHEMAS, ImmutableList.of( NAMESPACE ) )
                .concat( PostgresQuery.END );
        this.propertyTypesInSchemaQuery = PostgresQuery.selectColsFrom( PROPERTY_TYPES, ImmutableList.of( ID ) )
                .concat( PostgresQuery.WHERE ).concat( PostgresQuery.valueInArray( SCHEMA_LIST, true ) );
        this.entityTypesInSchemaQuery = PostgresQuery.selectColsFrom( ENTITY_TYPES, ImmutableList.of( ID ) )
                .concat( PostgresQuery.WHERE ).concat( PostgresQuery.valueInArray( SCHEMA_LIST, true ) );
    }

    private Set<UUID> getElementsInSchema( FullQualifiedName schemaName, String query ) {
        try ( Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( query ) ) {
            Set<UUID> result = Sets.newHashSet();
            ps.setString( 1, schemaName.toString() );
            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.id( rs ) );
            }
            connection.close();
            return result;
        } catch ( SQLException e ) {
            logger.debug( "Unable to load EDM elements of schema {}.", schemaName, e );
            return ImmutableSet.of();
        }
    }

    @Nonnull @Override public Set<UUID> getAllPropertyTypesInSchema( FullQualifiedName schemaName ) {
        return getElementsInSchema( schemaName, propertyTypesInSchemaQuery );
    }

    @Nonnull @Override public Set<UUID> getAllEntityTypesInSchema( FullQualifiedName schemaName ) {
        return getElementsInSchema( schemaName, entityTypesInSchemaQuery );
    }

    @Nonnull @Override public Iterable<String> getNamespaces() {
        try ( Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( getNamespaces ) ) {
            List<String> result = Lists.newArrayList();
            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.namespace( rs ) );
            }
            connection.close();
            return result;
        } catch ( SQLException e ) {
            logger.debug( "Unable to get all namespaces.", e );
            return ImmutableList.of();
        }
    }
}
