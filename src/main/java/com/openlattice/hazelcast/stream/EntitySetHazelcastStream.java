package com.openlattice.hazelcast.stream;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.EntityDataValue;
import com.openlattice.hazelcast.aggregators.EntitySetAggregator;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntitySetHazelcastStream extends HazelcastStream<EntityDataValue, EntityDataKey, EntityDataValue> {
    public EntitySetHazelcastStream( HazelcastInstance hazelcastInstance ) {
        super( hazelcastInstance );
    }

    @Override
    public ListenableFuture<Long> start(
            ListeningExecutorService executorService,
            IMap<EntityDataKey, EntityDataValue> map,
            Predicate p ) {
        final UUID streamId = getStreamId();
        return executorService.submit( () -> map.aggregate( new EntitySetAggregator( streamId ), p ) );
    }
}
