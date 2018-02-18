/*
 * Copyright (C) 2017. OpenLattice, Inc
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
import com.openlattice.organization.OrganizationPrincipal;
import java.io.IOException;
import org.springframework.stereotype.Component;

@Component
public class OrganizationPrincipalStreamSerializer extends  SecurablePrincipalStreamSerializer {
    @Override public Class<? extends SecurablePrincipal> getClazz() {
        return OrganizationPrincipal.class;
    }

    @Override public OrganizationPrincipal read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ORGANIZATION_PRINCIPAL.ordinal();
    }

    public static OrganizationPrincipal deserialize( ObjectDataInput in ) throws IOException {
        return (OrganizationPrincipal) SecurablePrincipalStreamSerializer.deserialize( in );
    }
}
