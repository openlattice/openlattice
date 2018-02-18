

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

package com.openlattice.linking.mapstores;

import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.linking.LinkingVertexKey;
import com.openlattice.linking.WeightedLinkingVertexKeySet;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.openlattice.conductor.codecs.odata.Table;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;
import org.apache.commons.lang.NotImplementedException;

import java.util.Collection;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class LinkingEdgesMapstore
        extends AbstractStructuredCassandraMapstore<LinkingVertexKey, WeightedLinkingVertexKeySet> {
    public LinkingEdgesMapstore( Session session ) {
        super( HazelcastMap.LINKING_EDGES.name(), session, Table.WEIGHTED_LINKING_EDGES.getBuilder() );
    }

    public static LinkingVertexKey testKey() {
        return new LinkingVertexKey( UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override
    protected BoundStatement bind( LinkingVertexKey key, BoundStatement bs ) {
        throw new NotImplementedException( "How did I get here? This is bad. " );
        //        final LinkingVertexKey src = key.getSrc();
        //        final LinkingVertexKey dst = key.getDst();
        //        return bs.setUUID( CommonColumns.GRAPH_ID.cql(), key.getGraphId() )
        //                .setUUID( CommonColumns.SOURCE_LINKING_VERTEX_ID.cql(), src.getVertexId() )
        //                .setUUID( CommonColumns.DESTINATION_LINKING_VERTEX_ID.cql(), dst.getVertexId() );
    }

    @Override
    protected BoundStatement bind( LinkingVertexKey key, WeightedLinkingVertexKeySet value, BoundStatement bs ) {
        throw new NotImplementedException( "How did I get here? This is bad. This shouldn't happen." );
        //        return bind( key, bs ).setDouble( CommonColumns.EDGE_VALUE.cql(), value );

    }

    @Override protected Select.Where loadQuery() {
        return QueryBuilder.select( CommonColumns.GRAPH_ID.cql(),
                CommonColumns.SOURCE_LINKING_VERTEX_ID.cql(),
                CommonColumns.DESTINATION_LINKING_VERTEX_ID.cql(),
                CommonColumns.EDGE_VALUE.cql() )
                .from( Table.WEIGHTED_LINKING_EDGES.getKeyspace(), Table.WEIGHTED_LINKING_EDGES.getName() )
                .allowFiltering()
                .where( CommonColumns.GRAPH_ID.eq() )
                .and( CommonColumns.SOURCE_LINKING_VERTEX_ID.eq() )
                .and( CommonColumns.DESTINATION_LINKING_VERTEX_ID.eq() );
    }

    //    @Override protected RegularStatement deleteQuery() {
    //        return QueryBuilder.delete()
    //                .from( Table.LINKING_EDGES.getKeyspace(), Table.LINKING_EDGES.getName() )
    //                .where( QueryBuilder.gt( CommonColumns.EDGE_VALUE.cql(), CommonColumns.EDGE_VALUE.bindMarker() ) )
    //                .and( CommonColumns.GRAPH_ID.eq() )
    //                .and( CommonColumns.SOURCE_LINKING_VERTEX_ID.eq() )
    //                .and( CommonColumns.DESTINATION_LINKING_VERTEX_ID.eq() );
    //    }

    @Override public void delete( LinkingVertexKey key ) {
        throw new NotImplementedException( "How did I get here? This is bad. This shouldn't happen." );
        //        Double val = load( key );
        //        if( val != null ) {
        //            session.execute( bind( key, val, getDeleteQuery().bind() ) );
        //        }
    }

    @Override public MapConfig getMapConfig() {
        return super.getMapConfig()
                .addMapIndexConfig( new MapIndexConfig( "__key#graphId", false ) )
                .addMapIndexConfig( new MapIndexConfig( "__key#vertexId", false ) )
                .addMapIndexConfig( new MapIndexConfig( "value[any].vertexKey", false ) );
    }

    @Override public MapStoreConfig getMapStoreConfig() {
        return super.getMapStoreConfig()
                .setEnabled( false );
    }

    @Override
    public void deleteAll( Collection<LinkingVertexKey> keys ) {
        throw new NotImplementedException( "How did I get here? This is bad. This shouldn't happen." );
        //        keys.forEach( this::delete );
    }

    @Override
    protected LinkingVertexKey mapKey( Row row ) {
        throw new NotImplementedException( "How did I get here? This is bad. This shouldn't happen." );
        //        return GraphUtil.linkingEdge( row );
    }

    @Override
    protected WeightedLinkingVertexKeySet mapValue( ResultSet rs ) {
        throw new NotImplementedException( "How did I get here? This is bad. This shouldn't happen." );
        //        Row row = rs.one();
        //        return row == null ? null : GraphUtil.edgeValue( row );
    }

    @Override
    public LinkingVertexKey generateTestKey() {
        return testKey();
    }

    @Override
    public WeightedLinkingVertexKeySet generateTestValue() {
        return new WeightedLinkingVertexKeySet();
    }
}
