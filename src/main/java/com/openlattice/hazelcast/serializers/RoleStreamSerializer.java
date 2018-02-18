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

package com.openlattice.hazelcast.serializers;

import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.openlattice.authorization.SecurablePrincipal;
import com.openlattice.organization.roles.Role;
import java.io.IOException;
import org.springframework.stereotype.Component;

@Component
public class RoleStreamSerializer extends SecurablePrincipalStreamSerializer {
    @Override public Class<? extends SecurablePrincipal> getClazz() {
        return Role.class;
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ROLE.ordinal();
    }

    @Override public Role read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    public static Role deserialize( ObjectDataInput in ) throws IOException {
        //TODO: Split up securable principal stream serializer
        return (Role) SecurablePrincipalStreamSerializer.deserialize( in );
    }
}
