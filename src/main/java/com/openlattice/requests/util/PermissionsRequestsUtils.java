

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

package com.openlattice.requests.util;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.requests.PermissionsRequest;
import com.openlattice.requests.PermissionsRequestDetails;
import com.openlattice.requests.RequestStatus;
import com.datastax.driver.core.Row;
import com.openlattice.datastore.cassandra.RowAdapters;

public class PermissionsRequestsUtils {
    private PermissionsRequestsUtils() {}

    public static PermissionsRequest getPRFromRow( Row row ) {
        final List<UUID> aclRoot = RowAdapters.aclRoot( row );
        final Principal user = new Principal( PrincipalType.USER, RowAdapters.principalId( row ) );
        Map<UUID, EnumSet<Permission>> permissions = RowAdapters.aclChildrenPermissions( row );
        RequestStatus status = RowAdapters.reqStatus( row );
        return new PermissionsRequest( aclRoot, user, new PermissionsRequestDetails( permissions, status ) );
    }

}
