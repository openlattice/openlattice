package com.openlattice.hazelcast.aggregators;

import com.hazelcast.aggregation.Aggregator;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.EntityDataValue;
import com.openlattice.hazelcast.stream.HazelcastStreamSink;
import java.util.Map.Entry;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntitySetAggregator extends HazelcastStreamSink<EntityDataKey, EntityDataValue, Long, EntityDataValue> {
    private long count = 0;

    public EntitySetAggregator( UUID streamId ) {
        super( streamId );
    }

    @Override public void accumulate( Entry<EntityDataKey, EntityDataValue> input ) {
        insert( input.getValue() );
        count++;
    }

    @Override public void combine( Aggregator aggregator ) {
        count += ( (EntitySetAggregator) aggregator ).count;
    }

    @Override public Long aggregate() {
        return count;
    }
}
