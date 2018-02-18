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
import com.openlattice.data.analytics.IncrementableWeightId;
import com.openlattice.graph.aggregators.GraphCount;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
public class GraphCountStreamSerializer implements SelfRegisteringStreamSerializer<GraphCount> {
    @Override public Class<GraphCount> getClazz() {
        return GraphCount.class;
    }

    @Override public void write( ObjectDataOutput out, GraphCount object ) throws IOException {
        out.writeInt( object.getLimit() );
        UUIDStreamSerializer.serialize( out, object.getEntitySetId() );
        Map<UUID, IncrementableWeightId> weightedIds = object.getWeightedIds();
        long[] weights = new long[ weightedIds.size() ];
        long[] lsbs = new long[ weights.length ];
        long[] msbs = new long[ weights.length ];

        int i = 0;
        for ( IncrementableWeightId weightedId : weightedIds.values() ) {
            weights[ i ] = weightedId.getWeight();
            UUID id = weightedId.getId();
            lsbs[ i ] = id.getLeastSignificantBits();
            msbs[ i ] = id.getMostSignificantBits();
            ++i;
        }
        out.writeLongArray( weights );
        out.writeLongArray( lsbs );
        out.writeLongArray( msbs );
    }

    @Override public GraphCount read( ObjectDataInput in ) throws IOException {
        int limit = in.readInt();
        UUID entitySetId = UUIDStreamSerializer.deserialize( in );
        long[] weights = in.readLongArray();
        long[] lsbs = in.readLongArray();
        long[] msbs = in.readLongArray();
        Map<UUID, IncrementableWeightId> m = new HashMap<>( weights.length );

        for ( int i = 0; i < weights.length; i++ ) {
            UUID id = new UUID( msbs[ i ], lsbs[ i ] );
            m.put( id, new IncrementableWeightId( id, weights[ i ] ) );
        }
        return new GraphCount( limit, entitySetId, m );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.GRAPH_COUNT.ordinal();
    }

    @Override public void destroy() {

    }
}
