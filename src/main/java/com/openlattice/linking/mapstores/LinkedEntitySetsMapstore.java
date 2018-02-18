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
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import com.openlattice.conductor.codecs.odata.Table;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

public class LinkedEntitySetsMapstore
        extends AbstractStructuredCassandraPartitionKeyValueStore<UUID, DelegatedUUIDSet> {

    public LinkedEntitySetsMapstore( Session session ) {
        super( HazelcastMap.LINKED_ENTITY_SETS.name(), session, Table.LINKED_ENTITY_SETS.getBuilder() );
    }

    @Override
    public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override
    public DelegatedUUIDSet generateTestValue() {
        return DelegatedUUIDSet.wrap( ImmutableSet.of( UUID.randomUUID(), UUID.randomUUID() ) );
    }

    @Override
    protected BoundStatement bind( UUID key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ID.cql(), key );
    }

    @Override
    protected BoundStatement bind( UUID key, DelegatedUUIDSet value, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ID.cql(), key )
                .setSet( CommonColumns.ENTITY_SET_IDS.cql(), value, UUID.class );
    }

    @Override
    protected UUID mapKey( Row row ) {
        return row == null ? null : row.getUUID( CommonColumns.ID.cql() );
    }

    @Override
    protected DelegatedUUIDSet mapValue( ResultSet rs ) {
        Row row = rs.one();
        return row == null ? null
                : DelegatedUUIDSet.wrap( row.getSet( CommonColumns.ENTITY_SET_IDS.cql(), UUID.class ) );

    }

}
