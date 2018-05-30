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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.graph.edge.Edge;
import com.openlattice.graph.edge.EdgeKey;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
public class EdgeStreamSerializer implements SelfRegisteringStreamSerializer<Edge> {

    @Override public Class<Edge> getClazz() {
        return Edge.class;
    }

    @Override public void write( ObjectDataOutput out, Edge object ) throws IOException {
        EdgeKeyStreamSerializer.serialize( out, object.getKey() );
        out.writeLong( object.getVersion() );
        out.writeInt( object.getVersions().size() );
        for ( long v : object.getVersions() ) {
            out.writeLong( v );
        }
    }

    @Override public Edge read( ObjectDataInput in ) throws IOException {
        final EdgeKey key = EdgeKeyStreamSerializer.deserialize( in );

        final long version = in.readLong();
        final int size = in.readInt();
        final List<Long> versions = new ArrayList<>( size );
        for ( int i = 0; i < size; ++i ) {
            versions.add( in.readLong() );
        }

        return new Edge( key, version, versions );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.LOOM_EDGE.ordinal();
    }

    @Override public void destroy() {

    }
}
