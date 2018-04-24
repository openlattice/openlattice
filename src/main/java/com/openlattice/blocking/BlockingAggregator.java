package com.openlattice.blocking;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ICountDownLatch;
import com.openlattice.linking.HazelcastBlockingService;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.Map;
import java.util.UUID;

public class BlockingAggregator extends Aggregator<Map.Entry<GraphEntityPair, LinkingEntity>, Boolean>
        implements HazelcastInstanceAware {
    private static final long serialVersionUID = 2884032395472384002L;

    private transient HazelcastBlockingService     blockingService;
    private           UUID                         graphId;
    private           Iterable<UUID>               entitySetIds;
    private           Map<FullQualifiedName, UUID> propertyTypesIndexedByFqn;
    private transient ICountDownLatch              countDownLatch;

    private final int MAX_FAILED_CONSEC_ATTEMPTS = 5;

    public BlockingAggregator(
            UUID graphId,
            Iterable<UUID> entitySetIds,
            Map<FullQualifiedName, UUID> propertyTypesIndexedByFqn ) {
        this( graphId, entitySetIds, propertyTypesIndexedByFqn, null );
    }

    public BlockingAggregator(
            UUID graphId,
            Iterable<UUID> entitySetIds,
            Map<FullQualifiedName, UUID> propertyTypesIndexedByFqn,
            HazelcastBlockingService blockingService ) {
        this.graphId = graphId;
        this.entitySetIds = entitySetIds;
        this.propertyTypesIndexedByFqn = propertyTypesIndexedByFqn;
        this.blockingService = blockingService;
    }

    @Override public void accumulate( Map.Entry<GraphEntityPair, LinkingEntity> input ) {
        GraphEntityPair graphEntityPair = input.getKey();
        LinkingEntity linkingEntity = input.getValue();
        blockingService
                .blockAndMatch( graphEntityPair, linkingEntity, entitySetIds, propertyTypesIndexedByFqn );
    }

    @Override public void combine( Aggregator aggregator ) {
    }

    @Override public Boolean aggregate() {
        int numConsecFailures = 0;
        long count = countDownLatch.getCount();
        while ( count > 0 && numConsecFailures < MAX_FAILED_CONSEC_ATTEMPTS ) {
            try {
                Thread.sleep( 5000 );
                long newCount = countDownLatch.getCount();
                if ( newCount == count ) {
                    System.err.println( "Nothing is happening." );
                    numConsecFailures++;
                } else
                    numConsecFailures = 0;
                count = newCount;
            } catch ( InterruptedException e ) {
                System.err.println( "Error occurred while waiting for matching to finish." );
            }
        }
        if ( numConsecFailures == MAX_FAILED_CONSEC_ATTEMPTS )
            return false;
        return true;
    }

    @Override public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
        this.countDownLatch = hazelcastInstance.getCountDownLatch( graphId.toString() );
    }

    public UUID getGraphId() {
        return graphId;
    }

    public Iterable<UUID> getEntitySetIds() {
        return entitySetIds;
    }

    public Map<FullQualifiedName, UUID> getPropertyTypesIndexedByFqn() {
        return propertyTypesIndexedByFqn;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        BlockingAggregator that = (BlockingAggregator) o;

        if ( !graphId.equals( that.graphId ) )
            return false;
        return entitySetIds.equals( that.entitySetIds );
    }

    @Override public int hashCode() {
        int result = graphId.hashCode();
        result = 31 * result + entitySetIds.hashCode();
        return result;
    }
}
