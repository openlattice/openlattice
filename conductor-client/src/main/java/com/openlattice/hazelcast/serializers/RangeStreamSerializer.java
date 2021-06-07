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
 *
 */

package com.openlattice.hazelcast.serializers;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.openlattice.ids.Range;
import java.io.IOException;
import org.springframework.stereotype.Component;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
public class RangeStreamSerializer implements SelfRegisteringStreamSerializer<Range> {
    @Override public Class<Range> getClazz() {
        return Range.class;
    }

    @Override public void write( ObjectDataOutput out, Range object ) throws IOException {
        out.writeLong(  object.getBase() );
        out.writeLong(  object.getMsb() );
        out.writeLong(  object.getLsb() );
    }

    @Override public Range read( ObjectDataInput in ) throws IOException {
        return new Range( in.readLong(), in.readLong(), in.readLong() );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.RANGE.ordinal();
    }

    @Override public void destroy() {

    }
}
