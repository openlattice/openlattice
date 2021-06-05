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

package com.openlattice.edm.properties;

import com.dataloom.streams.StreamUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.postgres.PostgresColumn;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

public class PostgresTypeManager {
    private static final Logger           logger = LoggerFactory.getLogger( PostgresTypeManager.class );
    private final        HikariDataSource hds;

    private final String getEntityTypes;
    private final String getPropertyTypes;
    private final String getAssociationEntityTypes;
    private final String getAssociationTypeIds;
    private final String getEntityTypesStrict;
    private final String getEnumTypeIds;

    private final String entityTypesContainPropertyType;
    private final String getPropertyTypesInNamespace;
    private final String getEntityTypeChildIds;
    private final String getAssociationTypeIdsForEntityTypeId;

    public PostgresTypeManager( HikariDataSource hds ) {
        this.hds = hds;

        // Table names
        String ENTITY_TYPES = PostgresTable.ENTITY_TYPES.getName();
        String PROPERTY_TYPES = PostgresTable.PROPERTY_TYPES.getName();
        String ASSOCIATION_TYPES = PostgresTable.ASSOCIATION_TYPES.getName();
        String ENUM_TYPES = PostgresTable.ENUM_TYPES.getName();

        // Property names
        String ID = PostgresColumn.ID.getName();
        String PROPERTIES = PostgresColumn.PROPERTIES.getName();
        String NAMESPACE = PostgresColumn.NAMESPACE.getName();
        String CATEGORY = PostgresColumn.CATEGORY.getName();
        String BASE_TYPE = PostgresColumn.BASE_TYPE.getName();
        String SRC = PostgresColumn.SRC.getName();
        String DST = PostgresColumn.DST.getName();

        // SQL statements
        this.getEntityTypes = "SELECT * FROM ".concat( ENTITY_TYPES ).concat( ";" );
        this.getPropertyTypes = "SELECT * FROM ".concat( PROPERTY_TYPES ).concat( ";" );
        this.getAssociationEntityTypes = "SELECT * FROM ".concat( ENTITY_TYPES ).concat( " WHERE " )
                .concat( CATEGORY )
                .concat( " = '" ).concat(
                        SecurableObjectType.AssociationType.name() ).concat( "';" );
        this.getAssociationTypeIds = "SELECT ".concat( ID ).concat( " FROM " ).concat( ASSOCIATION_TYPES )
                .concat( ";" );
        this.getEntityTypesStrict = "SELECT * FROM ".concat( ENTITY_TYPES ).concat( " WHERE " ).concat( CATEGORY )
                .concat( " = '" ).concat(
                        SecurableObjectType.EntityType.name() ).concat( "';" );
        this.getEnumTypeIds = "SELECT ".concat( ID ).concat( " FROM " ).concat( ENUM_TYPES ).concat( ";" );
        this.entityTypesContainPropertyType = "SELECT * FROM ".concat( ENTITY_TYPES ).concat( " WHERE ? = ANY(" )
                .concat( PROPERTIES ).concat( ");" );
        this.getPropertyTypesInNamespace = "SELECT * FROM ".concat( PROPERTY_TYPES ).concat( " WHERE " )
                .concat( NAMESPACE ).concat( " = ?;" );
        this.getEntityTypeChildIds = "SELECT ".concat( ID ).concat( " FROM " ).concat( ENTITY_TYPES )
                .concat( " WHERE " ).concat( BASE_TYPE ).concat( " = ?;" );
        this.getAssociationTypeIdsForEntityTypeId = "SELECT ".concat( ID )
                .concat( " FROM " ).concat( ASSOCIATION_TYPES ).concat( " WHERE ? = ANY(" ).concat( SRC )
                .concat( ") OR ? = ANY(" ).concat( DST ).concat( ");" );
    }

    public Iterable<PropertyType> getPropertyTypesInNamespace( String namespace ) {
        try ( Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( getPropertyTypesInNamespace ) ) {
            List<PropertyType> result = Lists.newArrayList();
            ps.setString( 1, namespace );
            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.propertyType( rs ) );
            }

            connection.close();
            return result;
        } catch ( SQLException e ) {
            logger.debug( "Unable to get property types in namespace.", e );
            return ImmutableList.of();
        }
    }

    public Iterable<PropertyType> getPropertyTypes() {
        try ( Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( getPropertyTypes ) ) {
            List<PropertyType> result = Lists.newArrayList();
            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.propertyType( rs ) );
            }

            connection.close();
            return result;
        } catch ( SQLException e ) {
            logger.debug( "Unable to load all property types", e );
            return ImmutableList.of();
        }
    }

    private Iterable<EntityType> getEntityTypesForQuery( String query ) {
        try ( Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( query ) ) {
            List<EntityType> result = Lists.newArrayList();
            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.entityType( rs ) );
            }

            connection.close();
            return result;
        } catch ( SQLException e ) {
            logger.debug( "Unable to load entity types for query: {}", query, e );
            return ImmutableList.of();
        }
    }

    public Iterable<UUID> getIdsForQuery( String query ) {
        try ( Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( query ) ) {
            List<UUID> result = Lists.newArrayList();
            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.id( rs ) );
            }

            connection.close();
            return result;
        } catch ( SQLException e ) {
            logger.debug( "Unable to load ids for query: {}", query, e );
            return ImmutableList.of();
        }
    }

    public Iterable<EntityType> getEntityTypes() {
        return getEntityTypesForQuery( getEntityTypes );
    }

    public Iterable<EntityType> getEntityTypesStrict() {
        return getEntityTypesForQuery( getEntityTypesStrict );
    }

    public Iterable<EntityType> getAssociationEntityTypes() {
        return getEntityTypesForQuery( getAssociationEntityTypes );
    }

    public Stream<UUID> getAssociationIdsForEntityType( UUID entityTypeId ) {
        try ( Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( getAssociationTypeIdsForEntityTypeId ) ) {
            List<UUID> associationTypeIds = Lists.newArrayList();

            ps.setObject( 1, entityTypeId );
            ps.setObject( 2, entityTypeId );

            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                associationTypeIds.add( ResultSetAdapters.id( rs ) );
            }

            connection.close();
            return associationTypeIds.stream();
        } catch ( SQLException e ) {
            logger.debug( "Unable to load src and dst association types for entity type id", e );
            return Stream.empty();
        }
    }

    public Iterable<UUID> getAssociationTypeIds() {
        return getIdsForQuery( getAssociationTypeIds );
    }

    public Stream<UUID> getEnumTypeIds() {
        return StreamUtil.stream( getIdsForQuery( getEnumTypeIds ) );
    }

    public Stream<EntityType> getEntityTypesContainingPropertyTypesAsStream( Set<UUID> properties ) {
        return properties.parallelStream().map( this::getEntityTypesContainingPropertyType )
                .flatMap( StreamUtil::stream );
    }

    @Deprecated
    private Iterable<EntityType> getEntityTypesContainingPropertyType( UUID propertyId ) {
        try ( Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( entityTypesContainPropertyType ) ) {
            List<EntityType> result = Lists.newArrayList();
            ps.setObject( 1, propertyId );
            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.entityType( rs ) );
            }
            connection.close();
            return result;
        } catch ( SQLException e ) {
            logger.debug( "Unable to load entity types containing property type: {}", propertyId.toString(), e );
            return ImmutableList.of();
        }
    }

    private Iterable<UUID> getEntityTypeChildrenIds( UUID entityTypeId ) {
        try ( Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( getEntityTypeChildIds ) ) {
            List<UUID> result = Lists.newArrayList();
            ps.setObject( 1, entityTypeId );

            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.id( rs ) );
            }

            connection.close();
            return result;
        } catch ( SQLException e ) {
            logger.debug( "Unable to load base entity types for type: {}", entityTypeId.toString(), e );
            return ImmutableList.of();
        }
    }

    public Stream<UUID> getEntityTypeChildrenIdsDeep( UUID entityTypeId ) {
        Set<UUID> children = Sets.newHashSet();
        Queue<UUID> idsToLoad = Queues.newArrayDeque();
        idsToLoad.add( entityTypeId );
        while ( !idsToLoad.isEmpty() ) {
            UUID id = idsToLoad.poll();
            getEntityTypeChildrenIds( id ).forEach( idsToLoad::add );
            children.add( id );
        }
        return StreamUtil.stream( children );
    }

}
