package com.openlattice.hazelcast.aggregators;

import com.hazelcast.aggregation.Aggregator;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.EntityDataValue;
import com.openlattice.hazelcast.stream.HazelcastStreamSink;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntitySetAggregator extends HazelcastStreamSink<EntityDataKey, EntityDataValue, Long, EntityDataValue> {
    private long count;

    public EntitySetAggregator( UUID streamId ) {
        this( streamId,0 );
    }

    public EntitySetAggregator( UUID streamId, long count ) {
        super( streamId );
        this.count = count;
    }

    @Override public void accumulate( Entry<EntityDataKey, EntityDataValue> input ) {
        insert( input.getValue() );
        count++;
    }

    @Override public void combine( Aggregator aggregator ) {
        count += ( (EntitySetAggregator) aggregator ).count;
    }

    public long getCount() {
        return count;
    }

    @Override public Long aggregate() {
        close();
        return count;
    }

    @Override public String toString() {
        return "EntitySetAggregator{" +
                "count=" + count +
                '}';
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof EntitySetAggregator ) ) { return false; }
        EntitySetAggregator that = (EntitySetAggregator) o;
        return count == that.count;
    }

    @Override public int hashCode() {

        return Objects.hash( count );
    }
}
