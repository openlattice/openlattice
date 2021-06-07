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
import java.io.IOException;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
public class IncrementableWeightIdStreamSerializer
        implements SelfRegisteringStreamSerializer<IncrementableWeightId[]> {
    @Override
    public Class<IncrementableWeightId[]> getClazz() {
        return IncrementableWeightId[].class;
    }

    @Override
    public void write( ObjectDataOutput out, IncrementableWeightId[] object ) throws IOException {
        long[] weights = new long[ object.length ];
        long[] lsbs = new long[ weights.length ];
        long[] msbs = new long[ weights.length ];

        for ( int i = 0; i < object.length; ++i ) {
            weights[ i ] = object[ i ].getWeight();
            UUID id = object[ i ].getId();
            lsbs[ i ] = id.getLeastSignificantBits();
            msbs[ i ] = id.getMostSignificantBits();
        }

        out.writeLongArray( weights );
        out.writeLongArray( lsbs );
        out.writeLongArray( msbs );
    }

    @Override
    public IncrementableWeightId[] read( ObjectDataInput in ) throws IOException {
        long[] weights = in.readLongArray();
        long[] lsbs = in.readLongArray();
        long[] msbs = in.readLongArray();
        IncrementableWeightId[] weightedIds = new IncrementableWeightId[ weights.length ];

        for ( int i = 0; i < weightedIds.length; i++ ) {
            UUID id = new UUID( msbs[ i ], lsbs[ i ] );
            weightedIds[ i ] = new IncrementableWeightId( id, weights[ i ] );
        }
        return weightedIds;
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.INCREMENTABLE_WEIGHT_ID.ordinal();
    }

    @Override public void destroy() {

    }
}
