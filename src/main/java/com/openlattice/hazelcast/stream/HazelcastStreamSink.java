package com.openlattice.hazelcast.stream;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ILock;
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
    public static final  Eof                   EOF             = new Eof();
    private static final Map<Class<?>, Logger> subclassLoggers = new HashMap<>();
    private final Logger logger = subclassLoggers.computeIfAbsent( getClass(), LoggerFactory::getLogger );

    private final     UUID              streamId;
    private transient HazelcastInstance hazelcastInstance;
    private transient IQueue            stream;

    protected HazelcastStreamSink( UUID streamId ) {
        this.streamId = streamId;
    }

    public void insert( E element ) {
        try {
            stream.put( element );
        } catch ( InterruptedException e ) {
            logger.error( "Unable to insert element into the stream." );
        }
    }

    @Override public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
        this.hazelcastInstance = hazelcastInstance;
        this.stream = hazelcastInstance.getQueue( streamId.toString() );
    }

    public void close() {
        try {
            stream.put( EOF );
        } catch ( InterruptedException e ) {
            logger.error( "Unable to close stream {}. Destroying to avoid leak.", streamId, e );
            stream.destroy();
            ILock streamLock = hazelcastInstance.getLock( HazelcastStream.getStreamLockName( streamId ) );
            streamLock.unlock();
            streamLock.destroy();
            throw new IllegalStateException( "Unable to close the stream." );
        }
    }

    public UUID getStreamId() {
        return streamId;
    }

    public static class Eof {
    }
}
