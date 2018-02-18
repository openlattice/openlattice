

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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;
import com.openlattice.authorization.AceKey;
import com.openlattice.authorization.AceValue;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.authorization.util.AuthorizationUtils;
import com.openlattice.conductor.codecs.EnumSetTypeCodec;
import com.openlattice.conductor.codecs.odata.Table;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.openlattice.datastore.cassandra.RowAdapters;
import com.openlattice.hazelcast.HazelcastMap;
import java.util.EnumSet;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;

public class CassandraPermissionMapstore extends AbstractStructuredCassandraMapstore<AceKey, AceValue> {
    public CassandraPermissionMapstore( Session session ) {
        super( HazelcastMap.PERMISSIONS.name(), session, Table.PERMISSIONS.getBuilder() );
    }

    @Override
    protected BoundStatement bind( AceKey key, BoundStatement bs ) {
        return bs.setList( CommonColumns.ACL_KEYS.cql(), key.getAclKey(), UUID.class )
                .set( CommonColumns.PRINCIPAL_TYPE.cql(), key.getPrincipal().getType(), PrincipalType.class )
                .setString( CommonColumns.PRINCIPAL_ID.cql(), key.getPrincipal().getId() );
    }

    @Override
    protected BoundStatement bind( AceKey key, AceValue value, BoundStatement bs ) {
        EnumSet permissions = value.getPermissions();
        return bs.setList( CommonColumns.ACL_KEYS.cql(), key.getAclKey(), UUID.class )
                .set( CommonColumns.PRINCIPAL_TYPE.cql(), key.getPrincipal().getType(), PrincipalType.class )
                .setString( CommonColumns.PRINCIPAL_ID.cql(), key.getPrincipal().getId() )
                .set( CommonColumns.PERMISSIONS.cql(),
                        permissions,
                        EnumSetTypeCodec.getTypeTokenForEnumSetPermission() );
    }

    @Override
    protected AceKey mapKey( Row row ) {
        return AuthorizationUtils.aceKey( row );
    }

    @Override
    protected AceValue mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row != null ) {
            EnumSet<Permission> permissions = AuthorizationUtils.permissions( row );
            SecurableObjectType objectType = RowAdapters.securableObjectType( row );
            return new AceValue( permissions, objectType );
        }
        return null;
    }

    @Override
    public AceKey generateTestKey() {
        return new AceKey(
                new AclKey( UUID.randomUUID() ),
                new Principal( PrincipalType.USER, RandomStringUtils.randomAlphanumeric( 5 ) ) );
    }

    @Override
    public AceValue generateTestValue() {
        return new AceValue( EnumSet.of( Permission.READ, Permission.WRITE ),
                SecurableObjectType.PropertyTypeInEntitySet );
    }
}
