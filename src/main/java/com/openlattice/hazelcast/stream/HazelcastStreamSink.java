package com.openlattice.hazelcast.stream;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IQueue;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public abstract class HazelcastStreamSink<K, V, R, E> extends Aggregator<Entry<K, V>, R>
        implements HazelcastInstanceAware {
    private static final Map<Class<?>, Logger> subclassLoggers = new HashMap<>();

    private final Logger logger = subclassLoggers.computeIfAbsent( getClass(), LoggerFactory::getLogger );
    private final UUID                     streamId;
    private       IQueue<StreamElement<E>> stream;

    protected HazelcastStreamSink( UUID streamId ) {
        this.streamId = streamId;
    }

    public void insert( E element ) {
        try {
            stream.put( new StreamElement<E>( element ) );
        } catch ( InterruptedException e ) {
            logger.error( "Unable to insert element into the stream." );
        }
    }

    @Override public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
        this.stream = hazelcastInstance.getQueue( streamId.toString() );
    }
}
