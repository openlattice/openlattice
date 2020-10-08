

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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.ListStreamSerializers;
import com.openlattice.authorization.AclKey;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDList;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
public class AclKeyStreamSerializer extends ListStreamSerializers.DelegatedUUIDListStreamSerializer
        implements TestableSelfRegisteringStreamSerializer<DelegatedUUIDList> {
    @Override
    public Class<? extends DelegatedUUIDList> getClazz() {
        return AclKey.class;
    }

    @Override public AclKey read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.COMP_ACL_KEY.ordinal();
    }

    public static void serialize( ObjectDataOutput out, AclKey object ) throws IOException {
        ListStreamSerializers.fastUUIDListSerialize( out, object );
    }

    public static AclKey deserialize( ObjectDataInput in ) throws IOException {
        return new AclKey( ListStreamSerializers.fastUUIDArrayDeserialize( in ) );
    }

    @Override
    public DelegatedUUIDList generateTestValue() {
        return new AclKey( UUID.randomUUID(), UUID.randomUUID() );
    }
}
