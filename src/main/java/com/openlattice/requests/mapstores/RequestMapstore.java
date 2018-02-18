

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

package com.openlattice.requests.mapstores;

import com.openlattice.authorization.AceKey;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.authorization.util.AuthorizationUtils;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.requests.RequestStatus;
import com.openlattice.requests.Status;
import com.openlattice.requests.util.RequestUtil;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.openlattice.conductor.codecs.EnumSetTypeCodec;
import com.openlattice.conductor.codecs.odata.Table;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;

import java.util.UUID;

public class RequestMapstore extends AbstractStructuredCassandraMapstore<AceKey, Status> {
    private final Status TEST_STATUS = TestDataFactory.status();

    public RequestMapstore( Session session ) {
        super( HazelcastMap.REQUESTS.name(), session, Table.REQUESTS.getBuilder() );
    }

    @Override
    protected BoundStatement bind( AceKey key, BoundStatement bs ) {
        return bs.setList( CommonColumns.ACL_KEYS.cql(), key.getAclKey(), UUID.class )
                .set( CommonColumns.PRINCIPAL_TYPE.cql(), key.getPrincipal().getType(), PrincipalType.class )
                .setString( CommonColumns.PRINCIPAL_ID.cql(), key.getPrincipal().getId() );
    }

    @Override
    protected BoundStatement bind( AceKey key, Status status, BoundStatement bs ) {
        return bs.setList( CommonColumns.ACL_KEYS.cql(), key.getAclKey(), UUID.class )
                .set( CommonColumns.PRINCIPAL_TYPE.cql(), key.getPrincipal().getType(), PrincipalType.class )
                .setString( CommonColumns.PRINCIPAL_ID.cql(), key.getPrincipal().getId() )
                .set( CommonColumns.PERMISSIONS.cql(),
                        status.getRequest().getPermissions(),
                        EnumSetTypeCodec.getTypeTokenForEnumSetPermission() )
                .set( CommonColumns.STATUS.cql(), status.getStatus(), RequestStatus.class )
                .setString( CommonColumns.REASON.cql(), status.getRequest().getReason() );
    }

    @Override
    protected AceKey mapKey( Row row ) {
        return AuthorizationUtils.aceKey( row );
    }

    @Override
    protected Status mapValue( ResultSet rs ) {
        Row row = rs.one();
        return row == null ? null
                : RequestUtil.status( row );
    }

    @Override
    public AceKey generateTestKey() {
        return RequestUtil.aceKey( TEST_STATUS );
    }

    @Override
    public Status generateTestValue() {
        return TEST_STATUS;
    }
}
