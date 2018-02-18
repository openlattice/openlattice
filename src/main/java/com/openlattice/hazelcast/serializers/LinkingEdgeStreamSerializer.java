

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
import com.openlattice.linking.LinkingEdge;
import com.openlattice.linking.LinkingVertexKey;
import java.io.IOException;
import org.springframework.stereotype.Component;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

@Component
public class LinkingEdgeStreamSerializer implements SelfRegisteringStreamSerializer<LinkingEdge> {
    @Override
    public Class<? extends LinkingEdge> getClazz() {
        return LinkingEdge.class;
    }

    @Override
    public void write( ObjectDataOutput out, LinkingEdge object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public LinkingEdge read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.LINKING_EDGE.ordinal();
    }

    @Override
    public void destroy() {

    }

    public static void serialize( ObjectDataOutput out, LinkingEdge object ) throws IOException {
        LinkingVertexKeyStreamSerializer.serialize( out, object.getSrc() );
        LinkingVertexKeyStreamSerializer.serialize( out, object.getDst() );
    }

    public static LinkingEdge deserialize( ObjectDataInput in ) throws IOException {
        LinkingVertexKey src = LinkingVertexKeyStreamSerializer.deserialize( in );
        LinkingVertexKey dst = LinkingVertexKeyStreamSerializer.deserialize( in );
        return new LinkingEdge( src, dst );
    }
}
