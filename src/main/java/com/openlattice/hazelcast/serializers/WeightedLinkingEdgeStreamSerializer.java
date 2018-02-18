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
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.openlattice.linking.LinkingEdge;
import com.openlattice.linking.WeightedLinkingEdge;
import java.io.IOException;
import org.springframework.stereotype.Component;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
public class WeightedLinkingEdgeStreamSerializer implements SelfRegisteringStreamSerializer<WeightedLinkingEdge> {
    @Override public Class<WeightedLinkingEdge> getClazz() {
        return WeightedLinkingEdge.class;
    }

    @Override public void write( ObjectDataOutput out, WeightedLinkingEdge object ) throws IOException {
        serialize( out, object );
    }

    @Override public WeightedLinkingEdge read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.WEIGHTED_LINKING_EDGE.ordinal();
    }

    @Override public void destroy() {

    }

    public static void serialize( ObjectDataOutput out, WeightedLinkingEdge object ) throws IOException {
        LinkingEdgeStreamSerializer.serialize( out, object.getEdge() );
        out.writeDouble( object.getWeight() );
    }

    public static WeightedLinkingEdge deserialize( ObjectDataInput in ) throws IOException {
        LinkingEdge edge = LinkingEdgeStreamSerializer.deserialize( in );
        return new WeightedLinkingEdge( in.readDouble(), edge );
    }
}
