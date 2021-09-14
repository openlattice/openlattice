

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
import com.openlattice.edm.schemas.processors.SchemaMerger;

import java.io.DataInput;
import java.io.IOException;
import java.util.Set;
import org.springframework.stereotype.Component;

@Component
public class SchemaMergerStreamSerializer implements SelfRegisteringStreamSerializer<SchemaMerger> {

    @Override
    public void write( ObjectDataOutput out, SchemaMerger object ) throws IOException {
        SetStreamSerializers.serialize( out, object.getBackingCollection(), out::writeUTF );
    }

    @Override
    public SchemaMerger read( ObjectDataInput in ) throws IOException {
        Set<String> names = SetStreamSerializers.deserialize( in, DataInput::readUTF );
        return new SchemaMerger( names );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.SCHEMA_MERGER.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<SchemaMerger> getClazz() {
        return SchemaMerger.class;
    }

}
