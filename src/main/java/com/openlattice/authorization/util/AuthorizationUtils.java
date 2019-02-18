

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

package com.openlattice.authorization.util;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.datastax.driver.core.Row;
import com.openlattice.authorization.AceKey;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.datastore.cassandra.CommonColumns;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;

public final class AuthorizationUtils {
    private AuthorizationUtils() {
    }

    public static Principal getPrincipalFromRow( Row row ) {
        final PrincipalType principalType = row.get( CommonColumns.PRINCIPAL_TYPE.cql(), PrincipalType.class );
        final String principalId = row.getString( CommonColumns.PRINCIPAL_ID.cql() );
        checkState( StringUtils.isNotBlank( principalId ), "Principal id cannot be null." );
        checkNotNull( principalType, "Encountered null principal type" );
        return new Principal(
                principalType,
                principalId );
    }

    public static AceKey aceKey( Row row ) {
        Principal principal = getPrincipalFromRow( row );
        return new AceKey( aclKey( row ), principal );
    }

    public static AclKey aclKey( Row row ) {
        return new AclKey( row.getList( CommonColumns.ACL_KEYS.cql(), UUID.class ) );
    }

    public static SecurableObjectType securableObjectType( Row row ) {
        return row.get( CommonColumns.SECURABLE_OBJECT_TYPE.cql(), SecurableObjectType.class );
    }

    public static String reason( Row row ) {
        return row.getString( CommonColumns.REASON.cql() );
    }

    public static UUID getLastAclKeySafely( List<UUID> aclKeys ) {
        return aclKeys.isEmpty() ? null : aclKeys.get( aclKeys.size() - 1 );
    }

}
