

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
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.Iterables;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.conductor.codecs.odata.Table;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.openlattice.datastore.cassandra.RowAdapters;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CassandraTypeManager {
    private final Session           session;
    private final PreparedStatement entityTypesContainPropertyType;
    private final Select            getEntityTypeIds;
    private final Select            getEntityTypes;
    private final Select            getPropertyTypeIds;
    private final Select            getPropertyTypes;
    private final PreparedStatement getPropertyTypesInNamespace;

    private final PreparedStatement getComplexTypeIds;
    private final PreparedStatement getEnumTypeIds;
    private final PreparedStatement getEntityTypeChildIds;
    private final Select.Where      getAssociationEntityTypes;
    private final Select.Where      getAssociationTypeIds;
    private final Select.Where      getEntityTypesStrict;
    private final PreparedStatement getAssociationTypeIdsForSrc;
    private final PreparedStatement getAssociationTypeIdsForDst;

    public CassandraTypeManager( String keyspace, Session session ) {
        this.session = session;
        this.entityTypesContainPropertyType = session.prepare(
                QueryBuilder.select().all()
                        .from( keyspace, Table.ENTITY_TYPES.getName() ).allowFiltering()
                        .where( QueryBuilder
                                .contains( CommonColumns.PROPERTIES.cql(), CommonColumns.PROPERTIES.bindMarker() ) ) );
        this.getEntityTypeIds = QueryBuilder.select( CommonColumns.ID.cql() ).distinct().from( keyspace,
                Table.ENTITY_TYPES.getName() );
        this.getAssociationTypeIds = QueryBuilder.select( CommonColumns.ID.cql() ).from( keyspace,
                Table.ENTITY_TYPES.getName() ).allowFiltering()
                .where( QueryBuilder.eq( CommonColumns.CATEGORY.cql(), SecurableObjectType.AssociationType ) );
        this.getEntityTypes = QueryBuilder.select().all().from( keyspace,
                Table.ENTITY_TYPES.getName() );
        this.getPropertyTypeIds = QueryBuilder.select( CommonColumns.ID.cql() ).distinct().from( keyspace,
                Table.PROPERTY_TYPES.getName() );
        this.getPropertyTypes = QueryBuilder.select().all()
                .from( keyspace, Table.PROPERTY_TYPES.getName() );
        this.getPropertyTypesInNamespace = session.prepare(
                QueryBuilder.select().all()
                        .from( keyspace, Table.PROPERTY_TYPES.getName() )
                        .where( QueryBuilder
                                .eq( CommonColumns.NAMESPACE.cql(), CommonColumns.NAMESPACE.bindMarker() ) ) );
        this.getComplexTypeIds = session.prepare(
                QueryBuilder.select( CommonColumns.ID.cql() ).distinct()
                        .from( keyspace, Table.COMPLEX_TYPES.getName() ) );

        this.getEnumTypeIds = session.prepare(
                QueryBuilder.select( CommonColumns.ID.cql() ).distinct()
                        .from( keyspace, Table.ENUM_TYPES.getName() ) );
        this.getEntityTypeChildIds = session.prepare(
                QueryBuilder.select( CommonColumns.ID.cql() ).from( keyspace, Table.ENTITY_TYPES.getName() )
                        .allowFiltering().where(
                        QueryBuilder.eq( CommonColumns.BASE_TYPE.cql(),
                                CommonColumns.BASE_TYPE.bindMarker() ) ) );
        this.getAssociationEntityTypes = QueryBuilder.select().all().from( keyspace,
                Table.ENTITY_TYPES.getName() ).allowFiltering()
                .where( QueryBuilder.eq( CommonColumns.CATEGORY.cql(), SecurableObjectType.AssociationType ) );
        this.getEntityTypesStrict = QueryBuilder.select().all().from( keyspace,
                Table.ENTITY_TYPES.getName() ).allowFiltering()
                .where( QueryBuilder.eq( CommonColumns.CATEGORY.cql(), SecurableObjectType.EntityType ) );
        this.getAssociationTypeIdsForSrc = session.prepare( QueryBuilder.select().all().from( keyspace,
                Table.ASSOCIATION_TYPES.getName() ).allowFiltering()
                .where( QueryBuilder.contains( CommonColumns.SRC.cql(), CommonColumns.SRC.bindMarker() ) ) );
        this.getAssociationTypeIdsForDst = session.prepare( QueryBuilder.select().all().from( keyspace,
                Table.ASSOCIATION_TYPES.getName() ).allowFiltering()
                .where( QueryBuilder.contains( CommonColumns.DST.cql(), CommonColumns.DST.bindMarker() ) ) );
    }

    public Iterable<PropertyType> getPropertyTypesInNamespace( String namespace ) {
        return Iterables.transform(
                session.execute(
                        getPropertyTypesInNamespace.bind().setString( CommonColumns.NAMESPACE.cql(), namespace ) ),
                RowAdapters::propertyType );
    }

    public Iterable<PropertyType> getPropertyTypes() {
        return Iterables.transform( session.execute( getPropertyTypes ), RowAdapters::propertyType );
    }

    public Iterable<UUID> getPropertyTypeIds() {
        return Iterables.transform( session.execute( getPropertyTypeIds ),
                row -> row.getUUID( CommonColumns.ID.cql() ) );
    }

    public Iterable<UUID> getEntityTypeIds() {
        return Iterables.transform( session.execute( getEntityTypeIds ),
                row -> row.getUUID( CommonColumns.ID.cql() ) );
    }

    public Iterable<UUID> getAssociationTypeIds() {
        return Iterables.transform( session.execute( getAssociationTypeIds ),
                row -> row.getUUID( CommonColumns.ID.cql() ) );
    }



    public Stream<UUID> getAssociationIdsForEntityType( UUID entityTypeId ) {
        Iterable<UUID> srcAssociationIds = Iterables.transform(
                session.execute(
                        getAssociationTypeIdsForSrc.bind().setUUID( CommonColumns.SRC.cql(), entityTypeId ) ),
                row -> row.getUUID( CommonColumns.ID.cql() ) );
        Iterable<UUID> dstAssociationIds = Iterables.transform(
                session.execute(
                        getAssociationTypeIdsForDst.bind().setUUID( CommonColumns.DST.cql(), entityTypeId ) ),
                row -> row.getUUID( CommonColumns.ID.cql() ) );

        return Stream.concat( StreamUtil.stream( srcAssociationIds ), StreamUtil.stream( dstAssociationIds ) )
                .distinct();
    }

    public Stream<UUID> getComplexTypeIds() {
        return StreamUtil.stream( session.execute( getComplexTypeIds.bind() ) ).map( RowAdapters::id );
    }

    public Stream<UUID> getEnumTypeIds() {
        return StreamUtil.stream( session.execute( getEnumTypeIds.bind() ) ).map( RowAdapters::id );
    }



    private ResultSetFuture getEntityTypesContainingPropertyType( UUID propertyId ) {
        return session.executeAsync(
                entityTypesContainPropertyType.bind().setUUID( CommonColumns.PROPERTIES.cql(), propertyId ) );
    }

    private Iterable<UUID> getEntityTypeChildrenIds( UUID entityTypeId ) {
        return Iterables.transform(
                session.execute(
                        getEntityTypeChildIds.bind().setUUID( CommonColumns.BASE_TYPE.cql(), entityTypeId ) ),
                RowAdapters::id );
    }

    public Stream<UUID> getEntityTypeChildrenIdsDeep( UUID entityTypeId ) {
        Set<UUID> children = Sets.newHashSet();
        Queue<UUID> idsToLoad = Queues.newArrayDeque();
        idsToLoad.add( entityTypeId );
        while ( !idsToLoad.isEmpty() ) {
            UUID id = idsToLoad.poll();
            getEntityTypeChildrenIds( id ).forEach( childId -> idsToLoad.add( childId ) );
            children.add( id );
        }
        return StreamUtil.stream( children );
    }

}
