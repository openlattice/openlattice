

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

package com.openlattice.authorization.mapstores;

import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.authorization.util.AuthorizationUtils;
import com.openlattice.hazelcast.HazelcastMap;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.collect.ImmutableList;
import com.openlattice.conductor.codecs.odata.Table;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

import java.util.List;
import java.util.UUID;

public class SecurableObjectTypeMapstore
        extends AbstractStructuredCassandraPartitionKeyValueStore<List<UUID>, SecurableObjectType> {

    public SecurableObjectTypeMapstore( Session session ) {
        super( HazelcastMap.SECURABLE_OBJECT_TYPES.name(), session, Table.PERMISSIONS.getBuilder() );
    }

    @Override
    protected BoundStatement bind( List<UUID> key, BoundStatement bs ) {
        return bs.setList( CommonColumns.ACL_KEYS.cql(), key, UUID.class );
    }

    @Override
    protected BoundStatement bind( List<UUID> key, SecurableObjectType value, BoundStatement bs ) {
        return bs.setList( CommonColumns.ACL_KEYS.cql(), key, UUID.class )
                .set( CommonColumns.SECURABLE_OBJECT_TYPE.cql(), value, SecurableObjectType.class );
    }

    @Override
    protected List<UUID> mapKey( Row row ) {
        return AuthorizationUtils.aclKey( row );
    }

    @Override
    protected Update.Where storeQuery() {
        return QueryBuilder
                .update( Table.PERMISSIONS.getKeyspace(), Table.PERMISSIONS.getName() )
                .with( QueryBuilder.set( CommonColumns.SECURABLE_OBJECT_TYPE.cql(),
                        CommonColumns.SECURABLE_OBJECT_TYPE.bindMarker() ) )
                .where( QueryBuilder.eq( CommonColumns.ACL_KEYS.cql(), CommonColumns.ACL_KEYS.bindMarker() ) );
    }

    @Override
    protected SecurableObjectType mapValue( ResultSet rs ) {
        Row row = rs.one();
        return row == null ? null : AuthorizationUtils.securableObjectType( row );
    }

    @Override
    public List<UUID> generateTestKey() {
        return ImmutableList.of( UUID.randomUUID() );
    }

    @Override
    public SecurableObjectType generateTestValue() {
        return SecurableObjectType.EntitySet;
    }
}
