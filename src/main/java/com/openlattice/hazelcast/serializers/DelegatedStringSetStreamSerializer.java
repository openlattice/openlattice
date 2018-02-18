

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
import com.google.common.collect.Sets;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializer;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import java.io.IOException;
import org.springframework.stereotype.Component;

@Component
public class DelegatedStringSetStreamSerializer extends SetStreamSerializer<DelegatedStringSet, String> {

    public DelegatedStringSetStreamSerializer( ) {
        super( DelegatedStringSet.class );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.STRING_SET.ordinal();
    }

    @Override
    protected DelegatedStringSet newInstanceWithExpectedSize( int size ) {
        return DelegatedStringSet.wrap( Sets.newHashSetWithExpectedSize( size ) );
    }

    @Override
    protected String readSingleElement( ObjectDataInput in ) throws IOException {
        return in.readUTF();
    }

    @Override
    protected void writeSingleElement( ObjectDataOutput out, String element ) throws IOException {
        out.writeUTF( element );
    }

}
