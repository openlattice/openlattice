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

package com.openlattice.data.aggregators;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.hazelcast.aggregation.Aggregator;
import com.openlattice.data.hazelcast.DataKey;
import com.openlattice.data.hazelcast.Entities;
import com.openlattice.data.hazelcast.PropertyKey;
import java.nio.ByteBuffer;
import java.util.Map.Entry;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntitiesAggregator
        extends Aggregator<Entry<DataKey, ByteBuffer>, Entities> {
    private final SetMultimap<PropertyKey, ByteBuffer> byteBuffers = HashMultimap.create();

    @Override
    public void accumulate( Entry<DataKey, ByteBuffer> input ) {
        byteBuffers.put( input.getKey().toPropertyKey(), input.getValue() );
    }

    @Override
    public void combine( Aggregator aggregator ) {
        EntitiesAggregator other = (EntitiesAggregator) aggregator;
        this.byteBuffers.putAll( other.byteBuffers );

    }

    public SetMultimap<PropertyKey, ByteBuffer> getByteBuffers() {
        return byteBuffers;
    }

    @Override
    public Entities aggregate() {
        Entities entities = new Entities();
        byteBuffers
                .entries()
                .forEach( e -> {
                    PropertyKey key = e.getKey();
                    ByteBuffer val = e.getValue();
                    entities
                            .compute( key.getId(), ( k, v ) -> v == null ? HashMultimap.create() : v )
                            .put( key.getPropertyTypeId(), val );
                } );
        return entities;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof EntitiesAggregator ) ) { return false; }

        EntitiesAggregator that = (EntitiesAggregator) o;

        return byteBuffers.equals( that.byteBuffers );
    }

    @Override public int hashCode() {
        return byteBuffers.hashCode();
    }

    @Override public String toString() {
        return "EntitiesAggregator{" +
                "byteBuffers=" + byteBuffers +
                '}';
    }
}
