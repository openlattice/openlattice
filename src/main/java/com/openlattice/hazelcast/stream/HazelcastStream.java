package com.openlattice.hazelcast.stream;

import static com.openlattice.hazelcast.stream.HazelcastStreamSink.EOF;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.query.Predicate;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public abstract class HazelcastStream<T, K, V> implements Iterable<T> {
    private static final Map<Class<?>, Logger> subclassLoggers = new HashMap<>();

    private final Logger logger = subclassLoggers.computeIfAbsent( getClass(), LoggerFactory::getLogger );

    private UUID                     streamId;
    private ILock                    streamLock;
    private IQueue<StreamElement<T>> stream;

    protected HazelcastStream( HazelcastInstance hazelcastInstance ) {
        Pair<UUID, ILock> idAndLock = acquireSafeId( hazelcastInstance );
        this.streamId = idAndLock.getLeft();
        this.streamLock = idAndLock.getRight();
        this.stream = hazelcastInstance.getQueue( streamId.toString() );
    }

    public abstract ListenableFuture<Long> start(
            ListeningExecutorService executorService,
            IMap<K, V> map,
            Predicate p );

    @Override public Iterator<T> iterator() {
        HazelcastIterator<T> hzIterator = null;
        try {
            hzIterator = new HazelcastIterator<>( streamLock, streamId, stream );
        } catch ( InterruptedException e ) {
            logger.error( "Unable to create iterator for stream id {}", streamId, e );
        }
        return hzIterator;
    }

    public UUID getStreamId() {
        return streamId;
    }

    public ILock getStreamLock() {
        return streamLock;
    }

    public IQueue<StreamElement<T>> getStream() {
        return stream;
    }

    private Pair<UUID, ILock> acquireSafeId( HazelcastInstance hazelcastInstance ) {
        UUID id;
        ILock maybeStreamLock;
        do {
            id = UUID.randomUUID();
            maybeStreamLock = hazelcastInstance.getLock( getStreamLockName( id ) );
        } while ( !maybeStreamLock.tryLock() );

        return Pair.of( id, maybeStreamLock );
    }

    public static String getStreamLockName( UUID id ) {
        return "stream-" + id.toString();
    }

    public static class HazelcastIterator<T> implements Iterator<T> {
        private static final Logger logger = LoggerFactory.getLogger( HazelcastIterator.class );
        private final ILock  streamLock;
        private final UUID   streamId;
        private final IQueue stream;
        private final Lock lock = new ReentrantLock();
        private Object next;

        public HazelcastIterator(
                ILock streamLock,
                UUID streamId,
                IQueue stream ) throws InterruptedException {
            this.streamLock = streamLock;
            this.streamId = streamId;
            this.stream = stream;
            retrieveNext();
        }

        public UUID getStreamId() {
            return streamId;
        }

        @Override
        public boolean hasNext() {
            try {
                lock.lock();
                return next != EOF;
            } finally {
                lock.unlock();
            }
        }

        private void releaseId() {
            stream.destroy();
            streamLock.unlock();
            streamLock.destroy();
        }

        private void retrieveNext() throws InterruptedException {
            next = (T) stream.poll( 60, TimeUnit.SECONDS );
            if ( next == null ) {
                next = HazelcastStreamSink.EOF;
            }
        }

        @Override
        public T next() {
            final T nextElem;
            try {
                lock.lock();
                nextElem = (T) next;
                retrieveNext();
            } catch ( InterruptedException e ) {
                logger.error( "Unable to retrieve next element from stream queue {}", streamId, e );
                next = StreamElement.eof();
                throw new NoSuchElementException( "Unable to retrieve next element from " + streamId.toString() );
            } finally {
                lock.unlock();
            }

            if ( !hasNext() ) {
                releaseId();
            }

            return nextElem;
        }
    }
}
