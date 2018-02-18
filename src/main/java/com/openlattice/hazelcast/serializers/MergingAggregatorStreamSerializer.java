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
import com.openlattice.linking.aggregators.MergingAggregator;
import com.google.common.collect.Maps;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.linking.WeightedLinkingEdge;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
public class MergingAggregatorStreamSerializer implements SelfRegisteringStreamSerializer<MergingAggregator> {
    @Override
    public Class<MergingAggregator> getClazz() {
        return MergingAggregator.class;
    }

    @Override public void write( ObjectDataOutput out, MergingAggregator object ) throws IOException {
        if ( object.getSrcNeighborWeights() == null )
            out.writeInt( 0 );
        else
            serializeMap( out, object.getSrcNeighborWeights() );

        if ( object.getDstNeighborWeights() == null )
            out.writeInt( 0 );
        else
            serializeMap( out, object.getDstNeighborWeights() );

        WeightedLinkingEdgeStreamSerializer.serialize( out, object.getLightest() );
    }

    @Override public MergingAggregator read( ObjectDataInput in ) throws IOException {
        Map<UUID, Double> srcNW = deserializeMap( in );
        Map<UUID, Double> dstNW = deserializeMap( in );
        WeightedLinkingEdge lightest = WeightedLinkingEdgeStreamSerializer.deserialize( in );
        return new MergingAggregator( lightest, srcNW, dstNW );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.MERGER_AGGREGATOR.ordinal();
    }

    @Override public void destroy() {

    }

    public static void serializeMap( ObjectDataOutput out, Map<UUID, Double> m ) throws IOException {
        out.writeInt( m.size() );
        for ( Entry<UUID, Double> entry : m.entrySet() ) {
            UUIDStreamSerializer.serialize( out, entry.getKey() );
            out.writeDouble( entry.getValue() );
        }
    }

    public static Map<UUID, Double> deserializeMap( ObjectDataInput in ) throws IOException {
        int size = in.readInt();
        Map<UUID, Double> m = Maps.newHashMap();
        for ( int i = 0; i < size; i++ ) {
            UUID key = UUIDStreamSerializer.deserialize( in );
            double value = in.readDouble();
            m.put( key, value );
        }
        return m;
    }
}
