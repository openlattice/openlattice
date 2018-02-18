

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

import com.openlattice.authorization.AuthorizationManager;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.openlattice.conductor.codecs.odata.Table;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.openlattice.datastore.cassandra.RowAdapters;
import com.openlattice.edm.EntitySet;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;

public class CassandraEntitySetManager {
    private final String               keyspace;
    private final Session              session;
    private final AuthorizationManager authorizations;

    //    private final PreparedStatement getEntities;
    private final PreparedStatement getEntitySetsByType;
    private final PreparedStatement getEntitySet;
    private final Select            getAllEntitySets;

    public CassandraEntitySetManager( String keyspace, Session session, AuthorizationManager authorizations ) {
        Preconditions.checkArgument( StringUtils.isNotBlank( keyspace ), "Keyspace cannot be blank." );
        this.session = session;
        this.keyspace = keyspace;
        this.authorizations = authorizations;
        this.getEntitySet = session
                .prepare( QueryBuilder.select().all()
                        .from( this.keyspace, Table.ENTITY_SETS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.NAME.cql(), CommonColumns.NAME.bindMarker() ) ) );

        this.getEntitySetsByType = session
                .prepare( QueryBuilder.select().all()
                        .from( this.keyspace, Table.ENTITY_SETS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_TYPE_ID.cql(),
                                CommonColumns.ENTITY_TYPE_ID.bindMarker() ) ) );

        this.getAllEntitySets = QueryBuilder.select().all().from( keyspace, Table.ENTITY_SETS.getName() );

        //        this.getEntities = session
        //                .prepare( QueryBuilder.select()
        //                        .column( CommonColumns.ENTITY_SET_ID.cql() ).column( CommonColumns.ENTITYID.cql() )
        //                        .column( CommonColumns.SYNCID.cql() )
        //                        .from( keyspace, Table.DATA.getName() )
        //                        .where( QueryBuilder.in( PARTITION_INDEX.cql(), HazelcastEntityDatastore.PARTITION_INDEXES ) )
        //                        .and( CommonColumns.ENTITY_SET_ID.eq() )
        //                        .and( CommonColumns.SYNCID.eq() ) );
    }

    public EntitySet getEntitySet( String entitySetName ) {
        Row row = session.execute( getEntitySet.bind().setString( CommonColumns.NAME.cql(), entitySetName ) ).one();
        return row == null ? null : RowAdapters.entitySet( row );
    }

    /**
     * This method retrieve all entities (that may have been historical) in an entity set
     */
    @Deprecated
    public Iterable<String> getEntitiesInEntitySet( String entitySetName ) {
        //        ResultSet rs = session
        //                .execute( getEntities.bind()
        //                        .setUUID( CommonColumns.ENTITY_SET_ID.cql(),
        //                                getEntitySet( entitySetName ).getId() ) );
        //        return Iterables.transform( rs, row -> row.getString( CommonColumns.ENTITYID.cql() ) );
        return null;
    }

    public Iterable<EntitySet> getAllEntitySetsForType( UUID typeId ) {
        ResultSetFuture rsf = session.executeAsync(
                getEntitySetsByType.bind().setUUID( CommonColumns.ENTITY_TYPE_ID.cql(), typeId ) );
        return Iterables.transform( rsf.getUninterruptibly(), RowAdapters::entitySet );
    }

    public Iterable<EntitySet> getAllEntitySets() {
        ResultSetFuture rsf = session.executeAsync( getAllEntitySets );
        return Iterables.transform( rsf.getUninterruptibly(), RowAdapters::entitySet );
    }
}
