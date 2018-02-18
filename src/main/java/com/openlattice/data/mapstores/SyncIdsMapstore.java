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

import java.util.UUID;

import com.openlattice.hazelcast.HazelcastMap;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.openlattice.conductor.codecs.odata.Table;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

public class SyncIdsMapstore extends AbstractStructuredCassandraPartitionKeyValueStore<UUID, UUID> {
    private static final CassandraTableBuilder ctb = Table.SYNC_IDS.getBuilder();

    public SyncIdsMapstore( Session session ) {
        super( HazelcastMap.SYNC_IDS.name(), session, ctb );
    }

    @Override
    protected BoundStatement bind( UUID key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ENTITY_SET_ID.cql(), key );
    }

    @Override
    protected BoundStatement bind( UUID key, UUID value, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ENTITY_SET_ID.cql(), key )
                .setUUID( CommonColumns.SYNCID.cql(), value )
                .setUUID( CommonColumns.CURRENT_SYNC_ID.cql(), value );
    }

    @Override
    protected UUID mapKey( Row rs ) {
        return rs == null ? null : rs.getUUID( CommonColumns.ENTITY_SET_ID.cql() );
    }

    @Override
    protected UUID mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return row.getUUID( CommonColumns.CURRENT_SYNC_ID.cql() );
    }

    @Override
    public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override
    public UUID generateTestValue() {
        return UUIDs.timeBased();
    }
}
