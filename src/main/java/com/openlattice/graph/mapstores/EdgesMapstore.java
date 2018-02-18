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

package com.openlattice.graph.mapstores;

import com.openlattice.graph.core.Neighborhood;
import com.openlattice.graph.edge.Edge;
import com.openlattice.hazelcast.HazelcastMap;
import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.hazelcast.config.MapStoreConfig;
import com.openlattice.conductor.codecs.odata.Table;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.openlattice.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EdgesMapstore extends AbstractStructuredCassandraPartitionKeyValueStore<UUID, Neighborhood> {
    private static final Logger logger = LoggerFactory.getLogger( EdgesMapstore.class );

    public EdgesMapstore( Session session ) {
        super( HazelcastMap.EDGES.name(), session, Table.EDGES.getBuilder() );
    }

    @Override public void store( UUID key, Neighborhood value ) {
        logger.error( "Shouldn't ever be calling store for edges mapstore." );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public Neighborhood generateTestValue() {
        return Neighborhood.randomNeighborhood();
    }

    @Override
    protected BoundStatement bind( UUID key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.SRC_ENTITY_KEY_ID.cql(), key );
    }

    @Override
    protected BoundStatement bind( UUID key, Neighborhood value, BoundStatement bs ) {
        return null;
    }

    @Override
    protected UUID mapKey( Row rs ) {
        return rs.getUUID( CommonColumns.SRC_ENTITY_KEY_ID.cql() );
    }

    @Override
    public MapStoreConfig getMapStoreConfig() {
        return super.getMapStoreConfig();
    }

    @Override
    protected Neighborhood mapValue( ResultSet rs ) {
        Map<UUID, Map<UUID, SetMultimap<UUID, UUID>>> neighborhood = new HashMap<>();
        StreamUtil.stream( rs )
                .map( RowAdapters::loomEdge )
                .forEach( edge -> addToNeighborhood( edge, neighborhood ) );
        return new Neighborhood( neighborhood );
    }

    private static void addToNeighborhood( Edge e, Map<UUID, Map<UUID, SetMultimap<UUID, UUID>>> neighborhood ) {
        UUID dstTypeId = e.getKey().getDstTypeId();

        Map<UUID, SetMultimap<UUID, UUID>> m = neighborhood.get( dstTypeId );

        if ( m == null ) {
            m = new HashMap<>();
            neighborhood.put( dstTypeId, m );
        }

        UUID edgeTypeId = e.getKey().getEdgeTypeId();
        SetMultimap<UUID, UUID> sm = m.get( edgeTypeId );

        if ( sm == null ) {
            sm = HashMultimap.create();
            m.put( edgeTypeId, sm );
        }

        sm.put( e.getKey().getDstEntityKeyId(), e.getKey().getEdgeEntityKeyId() );

    }
}
