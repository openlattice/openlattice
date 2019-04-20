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
import com.openlattice.hazelcast.processors.UUIDKeyToUUIDSetMerger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import java.io.IOException;
import org.springframework.stereotype.Component;

@Component
public class UUIDKeyToUUIDSetMergerStreamSerializer implements SelfRegisteringStreamSerializer<UUIDKeyToUUIDSetMerger> {
    @Override public Class<? extends UUIDKeyToUUIDSetMerger> getClazz() {
        return UUIDKeyToUUIDSetMerger.class;
    }

    @Override public void write(
            ObjectDataOutput out, UUIDKeyToUUIDSetMerger object ) throws IOException {
        SetStreamSerializers.serialize(
                out,
                object.getBackingCollection(),
                elem -> UUIDStreamSerializer.serialize( out, elem ) );
    }

    @Override public UUIDKeyToUUIDSetMerger read( ObjectDataInput in ) throws IOException {
        return new UUIDKeyToUUIDSetMerger(
                SetStreamSerializers.deserialize( in, UUIDStreamSerializer::deserialize )
        );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ORGANIZATION_APP_MERGER.ordinal();
    }

    @Override public void destroy() {

    }
}
