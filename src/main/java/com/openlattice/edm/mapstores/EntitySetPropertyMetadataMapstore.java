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

import com.openlattice.edm.set.EntitySetPropertyKey;
import com.openlattice.edm.set.EntitySetPropertyMetadata;
import com.openlattice.hazelcast.HazelcastMap;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.openlattice.conductor.codecs.odata.Table;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.openlattice.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

public class EntitySetPropertyMetadataMapstore
        extends AbstractStructuredCassandraPartitionKeyValueStore<EntitySetPropertyKey, EntitySetPropertyMetadata> {
    private static final CassandraTableBuilder ctb = Table.ENTITY_SET_PROPERTY_METADATA.getBuilder();

    public EntitySetPropertyMetadataMapstore( Session session ) {
        super( HazelcastMap.ENTITY_SET_PROPERTY_METADATA.name(), session, ctb );
    }

    @Override
    protected BoundStatement bind( EntitySetPropertyKey key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ENTITY_SET_ID.cql(), key.getEntitySetId() )
                .setUUID( CommonColumns.PROPERTY_TYPE_ID.cql(), key.getPropertyTypeId() );
    }

    @Override
    protected BoundStatement bind( EntitySetPropertyKey key, EntitySetPropertyMetadata value, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ENTITY_SET_ID.cql(), key.getEntitySetId() )
                .setUUID( CommonColumns.PROPERTY_TYPE_ID.cql(), key.getPropertyTypeId() )
                .setString( CommonColumns.TITLE.cql(), value.getTitle() )
                .setString( CommonColumns.DESCRIPTION.cql(), value.getDescription() )
                .setBool( CommonColumns.SHOW.cql(), value.getDefaultShow() );
    }

    @Override
    protected EntitySetPropertyKey mapKey( Row rs ) {
        return rs == null ? null : RowAdapters.entitySetPropertyKey( rs );
    }

    @Override
    protected EntitySetPropertyMetadata mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return RowAdapters.entitySetPropertyMetadata( row );
    }

    @Override
    public EntitySetPropertyKey generateTestKey() {
        return new EntitySetPropertyKey( UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override
    public EntitySetPropertyMetadata generateTestValue() {
        return new EntitySetPropertyMetadata( "title", "description", true );
    }

}
