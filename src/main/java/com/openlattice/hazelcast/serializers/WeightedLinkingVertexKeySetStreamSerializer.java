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
import com.openlattice.linking.WeightedLinkingVertexKey;
import com.openlattice.linking.WeightedLinkingVertexKeySet;
import java.io.IOException;
import org.springframework.stereotype.Component;

@Component
public class WeightedLinkingVertexKeySetStreamSerializer
        implements SelfRegisteringStreamSerializer<WeightedLinkingVertexKeySet> {
    @Override public Class<? extends WeightedLinkingVertexKeySet> getClazz() {
        return WeightedLinkingVertexKeySet.class;
    }

    @Override public void write( ObjectDataOutput out, WeightedLinkingVertexKeySet object ) throws IOException {
        out.writeInt( object.size() );
        for ( WeightedLinkingVertexKey key : object ) {
            WeightedLinkingVertexKeyStreamSerializer.serialize( out, key );
        }
    }

    @Override public WeightedLinkingVertexKeySet read( ObjectDataInput in ) throws IOException {
        int size = in.readInt();
        WeightedLinkingVertexKeySet result = new WeightedLinkingVertexKeySet();
        for ( int i = 0; i < size; i++ ) {
            result.add( WeightedLinkingVertexKeyStreamSerializer.deserialize( in ) );
        }

        return result;
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.WEIGHTED_LINKING_VERTEX_KEY_SET.ordinal();
    }

    @Override public void destroy() {

    }
}
