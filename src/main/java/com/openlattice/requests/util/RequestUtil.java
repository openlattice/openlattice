

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

import com.openlattice.authorization.AceKey;
import com.openlattice.authorization.Principals;
import com.openlattice.requests.Request;
import com.openlattice.requests.RequestStatus;
import com.openlattice.requests.Status;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public final class RequestUtil {
    private RequestUtil() {
    }

    public static @Nonnull Status newStatus( @Nonnull Request request ) {
        return new Status(
                request.getAclKey(),
                request.getPermissions(),
                request.getReason(),
                Principals.getCurrentUser(),
                RequestStatus.SUBMITTED );
    }

    public static AceKey aceKey( Status status ) {
        return new AceKey( status.getRequest().getAclKey(), status.getPrincipal() );
    }

    public static Map<AceKey, Status> reqsAsStatusMap( Set<Request> requests ) {
        return requests
                .stream()
                .map( RequestUtil::newStatus )
                .collect( Collectors.toMap( RequestUtil::aceKey, Function.identity() ) );
    }

    public static Map<AceKey, Status> statusMap( Set<Status> requests ) {
        return requests
                .stream()
                .collect( Collectors.toMap( RequestUtil::aceKey, Function.identity() ) );
    }

    public static Status approve( Status s ) {
        return new Status( s.getRequest(), s.getPrincipal(), RequestStatus.APPROVED );
    }

    public static Status decline( Status s ) {
        return new Status( s.getRequest(), s.getPrincipal(), RequestStatus.DECLINED );
    }
}
