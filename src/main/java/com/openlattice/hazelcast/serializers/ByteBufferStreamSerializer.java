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
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.springframework.stereotype.Component;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
public class ByteBufferStreamSerializer  implements SelfRegisteringStreamSerializer<ByteBuffer> {

    @Override public Class<ByteBuffer> getClazz() {
        return ByteBuffer.class;
    }

    @Override public void write( ObjectDataOutput out, ByteBuffer object ) throws IOException {
        out.writeByteArray( object.array() );
    }

    @Override public ByteBuffer read( ObjectDataInput in ) throws IOException {
        return ByteBuffer.wrap( in.readByteArray() );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.BYTE_BUFFER.ordinal();
    }

    @Override public void destroy() {

    }
}
