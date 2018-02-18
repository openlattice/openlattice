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
import com.openlattice.edm.types.processors.AddPrimaryKeysToEntityTypeProcessor;
import java.io.IOException;
import org.springframework.stereotype.Component;

@Component
public class AddPrimaryKeysToEntityTypeProcessorStreamSerializer
        implements SelfRegisteringStreamSerializer<AddPrimaryKeysToEntityTypeProcessor> {
    @Override
    public Class<? extends AddPrimaryKeysToEntityTypeProcessor> getClazz() {
        return AddPrimaryKeysToEntityTypeProcessor.class;
    }

    @Override
    public void write(
            ObjectDataOutput out, AddPrimaryKeysToEntityTypeProcessor object ) throws IOException {
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getPropertyTypeIds() );
    }

    @Override
    public AddPrimaryKeysToEntityTypeProcessor read( ObjectDataInput in ) throws IOException {
        return new AddPrimaryKeysToEntityTypeProcessor( SetStreamSerializers.fastUUIDSetDeserialize( in ) );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ADD_PRIMARY_KEYS_TO_ENTITY_TYPE_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {
    }
}
