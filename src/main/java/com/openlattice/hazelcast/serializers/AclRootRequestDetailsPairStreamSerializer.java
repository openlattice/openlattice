

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
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.requests.AclRootRequestDetailsPair;
import com.openlattice.requests.PermissionsRequestDetails;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class AclRootRequestDetailsPairStreamSerializer implements SelfRegisteringStreamSerializer<AclRootRequestDetailsPair> {

    @Override
    public void write( ObjectDataOutput out, AclRootRequestDetailsPair object ) throws IOException {
        StreamSerializerUtils.serializeFromList( out, object.getAclRoot(), ( UUID key ) -> {
            UUIDStreamSerializer.serialize( out, key );
        } );
        PermissionsRequestDetailsStreamSerializer.serialize( out, object.getDetails() );
    }

    @Override
    public AclRootRequestDetailsPair read( ObjectDataInput in ) throws IOException {
        List<UUID> aclRoot = StreamSerializerUtils.deserializeToList( in, ( ObjectDataInput dataInput ) -> {
            return UUIDStreamSerializer.deserialize( dataInput );
        } );
        PermissionsRequestDetails details = PermissionsRequestDetailsStreamSerializer.deserialize( in );
        return new AclRootRequestDetailsPair( aclRoot, details );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ACLROOT_REQUEST_DETAILS_PAIR.ordinal();
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<AclRootRequestDetailsPair> getClazz() {
        return AclRootRequestDetailsPair.class;
    }

}
