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

package com.openlattice.edm.mapstores;

import com.openlattice.hazelcast.HazelcastMap;
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

public class EdmVersionMapstore extends AbstractStructuredCassandraPartitionKeyValueStore<String, UUID> {
    private static final CassandraTableBuilder ctb = Table.EDM_VERSIONS.getBuilder();

    public EdmVersionMapstore( Session session ) {
        super( HazelcastMap.EDM_VERSIONS.name(), session, ctb );
    }

    @Override
    protected BoundStatement bind( String key, BoundStatement bs ) {
        return bs.setString( CommonColumns.EDM_VERSION_NAME.cql(), key );
    }

    @Override
    protected BoundStatement bind( String key, UUID value, BoundStatement bs ) {
        return bs.setString( CommonColumns.EDM_VERSION_NAME.cql(), key )
                .setUUID( CommonColumns.EDM_VERSION.cql(), value );
    }

    @Override
    protected String mapKey( Row rs ) {
        return rs == null ? null : rs.getString( CommonColumns.EDM_VERSION_NAME.cql() );
    }

    @Override
    protected UUID mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return row.getUUID( CommonColumns.EDM_VERSION.cql() );
    }

    @Override
    public String generateTestKey() {
        return "edm";
    }

    @Override
    public UUID generateTestValue() {
        return UUIDs.timeBased();
    }
}
