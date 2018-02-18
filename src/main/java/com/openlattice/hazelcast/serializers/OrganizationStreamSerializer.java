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
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.authorization.Principal;
import com.openlattice.organization.Organization;
import com.openlattice.organization.OrganizationPrincipal;
import com.openlattice.organization.roles.Role;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class OrganizationStreamSerializer implements SelfRegisteringStreamSerializer<Organization> {
    @Override public Class<? extends Organization> getClazz() {
        return Organization.class;
    }

    @Override public void write( ObjectDataOutput out, Organization object ) throws IOException {
        serialize( out, object );
    }

    @Override public Organization read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ORGANIZATION.ordinal();
    }

    @Override public void destroy() {

    }

    public static void serialize( ObjectDataOutput out, Organization object ) throws IOException {
        SecurablePrincipalStreamSerializer.serialize( out, object.getSecurablePrincipal() );
        SetStreamSerializers.fastStringSetSerialize( out, object.getAutoApprovedEmails() );
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getApps() );
        SetStreamSerializers.serialize( out, object.getMembers(), (principal) -> {
            PrincipalStreamSerializer.serialize( out, principal );
        } );
        SetStreamSerializers.serialize( out, object.getRoles(), (role) -> {
            RoleStreamSerializer.serialize( out, role );
        } );
    }

    public static Organization deserialize( ObjectDataInput in ) throws IOException {
        OrganizationPrincipal securablePrincipal = (OrganizationPrincipal) SecurablePrincipalStreamSerializer.deserialize( in );
        Set<String> autoApprovedEmails = SetStreamSerializers.fastStringSetDeserialize( in );
        Set<UUID> apps = SetStreamSerializers.fastUUIDSetDeserialize( in );
        Set<Principal> members = SetStreamSerializers.deserialize( in, PrincipalStreamSerializer::deserialize );
        Set<Role> roles = SetStreamSerializers.deserialize( in, RoleStreamSerializer::deserialize );

        return new Organization( securablePrincipal, autoApprovedEmails, members, roles, apps );
    }
}
