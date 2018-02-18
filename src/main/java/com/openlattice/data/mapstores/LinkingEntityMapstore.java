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

package com.openlattice.data.mapstores;

import com.openlattice.blocking.GraphEntityPair;
import com.openlattice.blocking.LinkingEntity;
import com.openlattice.hazelcast.HazelcastMap;
import com.datastax.driver.core.*;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.openlattice.conductor.codecs.odata.Table;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class LinkingEntityMapstore
        extends AbstractStructuredCassandraMapstore<GraphEntityPair, LinkingEntity> {
    private static final CassandraTableBuilder ctb = Table.LINKING_ENTITIES.getBuilder();
    public static final String GRAPH_ID = "__key#graphId";
    public static final String ENTITY_KEY_ID = "__key#entityKeyId";
    // private static final Class<Set<String>> stringSetClass = (Class<Set<String>>) Sets.newHashSet().getClass();

    public LinkingEntityMapstore( Session session ) {
        super( HazelcastMap.LINKING_ENTITIES.name(), session, ctb );
    }

    @Override protected BoundStatement bind( GraphEntityPair key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.GRAPH_ID.cql(), key.getGraphId() )
                .setUUID( CommonColumns.ENTITY_KEY_ID.cql(), key.getEntityKeyId() );
    }

    @Override protected BoundStatement bind( GraphEntityPair key, LinkingEntity value, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.GRAPH_ID.cql(), key.getGraphId() )
                .setUUID( CommonColumns.ENTITY_KEY_ID.cql(), key.getEntityKeyId() )
                .setMap( CommonColumns.ENTITY.cql(), value.getEntity() );
    }

    @Override protected GraphEntityPair mapKey( Row rs ) {
        UUID graphId = rs.getUUID( CommonColumns.GRAPH_ID.cql() );
        UUID entityKeyId = rs.getUUID( CommonColumns.ENTITY_KEY_ID.cql() );
        return new GraphEntityPair( graphId, entityKeyId );
    }

    @Override protected LinkingEntity mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return new LinkingEntity( row.getMap( CommonColumns.ENTITY.cql(),
                TypeCodec.uuid().getJavaType(),
                TypeCodec.set( TypeCodec.varchar() ).getJavaType() )
                .entrySet().stream().collect(
                        Collectors.toMap( entry -> entry.getKey(),
                                entry -> DelegatedStringSet.wrap( (Set<String>) entry.getValue() ) ) ) );
    }

    @Override public GraphEntityPair generateTestKey() {
        return new GraphEntityPair( UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override public LinkingEntity generateTestValue() {
        Map<UUID, DelegatedStringSet> result = Maps.newHashMap();
        result.put( UUID.randomUUID(), DelegatedStringSet.wrap( ImmutableSet.of( "test" ) ) );
        return new LinkingEntity( result );
    }

//    @Override public MapStoreConfig getMapStoreConfig() {
//        return super.getMapStoreConfig().setWriteDelaySeconds( 5 );
//    }

    @Override
    public MapConfig getMapConfig() {
        return super.getMapConfig()
                .setInMemoryFormat( InMemoryFormat.OBJECT )
                .addMapIndexConfig( new MapIndexConfig( GRAPH_ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( ENTITY_KEY_ID, false ) );
    }
}
