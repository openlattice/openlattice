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

import java.util.UUID;

import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.linking.LinkingVertexKey;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.openlattice.conductor.codecs.odata.Table;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;

public class VertexIdsAfterLinkingMapstore extends AbstractStructuredCassandraMapstore<LinkingVertexKey, UUID> {
    public VertexIdsAfterLinkingMapstore( Session session ) {
        super( HazelcastMap.VERTEX_IDS_AFTER_LINKING.name(), session, Table.VERTEX_IDS_AFTER_LINKING.getBuilder() );
    }

    @Override
    protected BoundStatement bind( LinkingVertexKey key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.VERTEX_ID.cql(), key.getVertexId() )
                .setUUID( CommonColumns.GRAPH_ID.cql(), key.getGraphId() );
    }

    @Override
    protected BoundStatement bind( LinkingVertexKey key, UUID value, BoundStatement bs ) {
        return bind( key, bs ).setUUID( CommonColumns.NEW_VERTEX_ID.cql(), value );
    }

    @Override
    protected LinkingVertexKey mapKey( Row row ) {
        if ( row == null ) {
            return null;
        }
        UUID graphId = row.getUUID( CommonColumns.GRAPH_ID.cql() );
        UUID vertexId = row.getUUID( CommonColumns.VERTEX_ID.cql() );
        return new LinkingVertexKey( graphId, vertexId );
    }

    @Override
    protected UUID mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return row.getUUID( CommonColumns.NEW_VERTEX_ID.cql() );
    }

    @Override
    public Iterable<LinkingVertexKey> loadAllKeys() {
        //lazy loading
        return null;
    }
    
    @Override
    public LinkingVertexKey generateTestKey() {
        return new LinkingVertexKey( UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override
    public UUID generateTestValue() {
        return UUID.randomUUID();
    }
}
