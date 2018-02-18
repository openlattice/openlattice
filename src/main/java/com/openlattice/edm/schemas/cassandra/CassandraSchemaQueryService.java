

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

package com.openlattice.edm.schemas.cassandra;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;
import java.util.UUID;

import com.openlattice.conductor.codecs.odata.Table;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.openlattice.edm.schemas.SchemaQueryService;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.openlattice.datastore.cassandra.RowAdapters;

public class CassandraSchemaQueryService implements SchemaQueryService {
    private final Session           session;
    private final PreparedStatement propertyTypesInSchemaQuery;
    private final PreparedStatement entityTypesInSchemaQuery;
    private final RegularStatement  getNamespaces;

    public CassandraSchemaQueryService( String keyspace, Session session ) {
        this.session = checkNotNull( session, "Session cannot be null." );
        propertyTypesInSchemaQuery = session.prepare( getPropertyTypesInSchema( keyspace ) );
        entityTypesInSchemaQuery = session.prepare( getEntityTypesInSchema( keyspace ) );
        getNamespaces = QueryBuilder.select( CommonColumns.NAMESPACE.cql() ).distinct()
                .from( Table.SCHEMAS.getKeyspace(), Table.SCHEMAS.getName() );
    }

    private static RegularStatement getPropertyTypesInSchema( String keyspace ) {
        return QueryBuilder
                .select( CommonColumns.ID.cql() )
                .from( keyspace, Table.PROPERTY_TYPES.getName() ).allowFiltering()
                .where( QueryBuilder.contains( CommonColumns.SCHEMAS.cql(), CommonColumns.SCHEMAS.bindMarker() ) );
    }

    private static RegularStatement getEntityTypesInSchema( String keyspace ) {
        return QueryBuilder
                .select( CommonColumns.ID.cql() )
                .from( keyspace, Table.ENTITY_TYPES.getName() ).allowFiltering()
                .where( QueryBuilder.contains( CommonColumns.SCHEMAS.cql(), CommonColumns.SCHEMAS.bindMarker() ) );
    }

    /*
     * (non-Javadoc)
     * @see
     * com.dataloom.edm.schemas.cassandra.SchemaQueryService#getAllPropertyTypesInSchema(org.apache.olingo.commons.api.
     * edm.FullQualifiedName)
     */
    @Override
    public Set<UUID> getAllPropertyTypesInSchema( FullQualifiedName schemaName ) {
        ResultSet propertyTypes = session.execute(
                propertyTypesInSchemaQuery.bind()
                        .set( CommonColumns.SCHEMAS.cql(), schemaName, FullQualifiedName.class ) );
        return ImmutableSet.copyOf( Iterables.transform( propertyTypes, RowAdapters::id ) );
    }

    /*
     * (non-Javadoc)
     * @see
     * com.dataloom.edm.schemas.cassandra.SchemaQueryService#getAllEntityTypesInSchema(org.apache.olingo.commons.api.edm
     * .FullQualifiedName)
     */
    @Override
    public Set<UUID> getAllEntityTypesInSchema( FullQualifiedName schemaName ) {
        ResultSet propertyTypes = session.execute(
                entityTypesInSchemaQuery.bind()
                        .set( CommonColumns.SCHEMAS.cql(), schemaName, FullQualifiedName.class ) );
        return ImmutableSet.copyOf( Iterables.transform( propertyTypes, RowAdapters::id ) );
    }

    /*
     * (non-Javadoc)
     * @see com.dataloom.edm.schemas.SchemaQueryService#getNamespaces()
     */
    @Override
    public Iterable<String> getNamespaces() {
        return Iterables.transform( session.execute( getNamespaces ), RowAdapters::namespace );
    }
}
