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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;
import com.openlattice.conductor.codecs.odata.Table;
import com.openlattice.data.EntityKey;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.linking.LinkingEntityKey;
import com.openlattice.mapstores.TestDataFactory;
import java.util.UUID;

public class LinkingEntityVerticesMapstore extends AbstractStructuredCassandraMapstore<LinkingEntityKey, UUID> {
    public LinkingEntityVerticesMapstore( Session session ) {
        super( HazelcastMap.LINKING_ENTITY_VERTICES.name(), session, Table.LINKING_ENTITY_VERTICES.getBuilder() );
    }

    @Override
    protected BoundStatement bind( LinkingEntityKey key, BoundStatement bs ) {
        EntityKey ek = key.getEntityKey();
        return bs.setUUID( CommonColumns.ENTITY_SET_ID.cql(), ek.getEntitySetId() )
                .setString( CommonColumns.ENTITYID.cql(), ek.getEntityId() )
                .setUUID( CommonColumns.GRAPH_ID.cql(), key.getGraphId() );
    }

    @Override
    protected BoundStatement bind( LinkingEntityKey key, UUID value, BoundStatement bs ) {
        return bind( key, bs ).setUUID( CommonColumns.VERTEX_ID.cql(), value );

    }

    @Override
    protected LinkingEntityKey mapKey( Row row ) {
        if ( row == null ) {
            return null;
        }
        UUID entitySetId = row.getUUID( CommonColumns.ENTITY_SET_ID.cql() );
        UUID graphId = row.getUUID( CommonColumns.GRAPH_ID.cql() );
        String entityId = row.getString( CommonColumns.ENTITYID.cql() );
        return new LinkingEntityKey( graphId, new EntityKey( entitySetId, entityId ) );
    }

    @Override
    protected UUID mapValue( ResultSet rs ) {
        Row row = rs.one();
        return row == null ? null : row.getUUID( CommonColumns.VERTEX_ID.cql() );
    }

    @Override
    public LinkingEntityKey generateTestKey() {
        return new LinkingEntityKey( UUID.randomUUID(), TestDataFactory.entityKey() );
    }

    @Override
    public UUID generateTestValue() {
        return UUID.randomUUID();
    }
}