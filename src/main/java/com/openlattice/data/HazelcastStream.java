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

package com.openlattice.data;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IQueue;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public abstract class HazelcastStream<T> implements Iterable<T> {
    private static final Logger logger = LoggerFactory.getLogger( HazelcastStream.class );
    private final HazelcastInstance        hazelcastInstance;
    private final ListeningExecutorService executor;

    public HazelcastStream( ListeningExecutorService executor, HazelcastInstance hazelcastInstance ) {
        this.hazelcastInstance = hazelcastInstance;
        this.executor = executor;
    }

    protected abstract long buffer( UUID streamId );

    @Override public Iterator<T> iterator() {
        HazelcastIterator hzIterator = new HazelcastIterator<T>( executor, hazelcastInstance );
        executor.execute( () -> {
            Stopwatch w = Stopwatch.createStarted();
            hzIterator.setLength( buffer( hzIterator.getStreamId() ) );
            logger.info( "Buffering time for {}: {} ms",
                    getClass().getCanonicalName(),
                    w.elapsed( TimeUnit.MILLISECONDS ) );
        } );
        return hzIterator;
    }

    public static class HazelcastIterator<T> implements Iterator<T> {
        private final ILock     streamLock;
        private final UUID      streamId;
        private final IQueue<T> stream;
        private final Lock lock = new ReentrantLock();
        private final ListeningExecutorService executor;
        private long length   = -1;
        private long position = 0;
        private ListenableFuture<T> fNext;

        public HazelcastIterator( ListeningExecutorService executor, HazelcastInstance hazelcastInstance ) {
            Pair<UUID, ILock> idAndLock = acquireSafeId( hazelcastInstance );
            this.streamId = idAndLock.getLeft();
            this.streamLock = idAndLock.getRight();
            this.stream = hazelcastInstance.getQueue( streamId.toString() );
            this.executor = executor;
            fNext = load();
        }

        private Pair<UUID, ILock> acquireSafeId( HazelcastInstance hazelcastInstance ) {
            UUID id;
            ILock maybeStreamLock;
            do {
                id = UUID.randomUUID();
                maybeStreamLock = hazelcastInstance.getLock( id.toString() );
            } while ( !maybeStreamLock.tryLock() );

            return Pair.of( id, maybeStreamLock );
        }

        void setLength( long length ) {
            this.length = length;
            if ( position >= length ) {
                fNext.cancel( true );
            }
        }

        public UUID getStreamId() {
            return streamId;
        }

        @Override
        public boolean hasNext() {
            /*
             * Until we have the full length we have to try and read the nextAvailable element to determine and keep track
             * of the current position.
             *
             * Once we have the full length we can determine if there is more available.
             */

            if ( length < 0 ) {
                try {
                    lock.lock();
                    return fNext.get() != null;
                } catch ( InterruptedException | ExecutionException e ) {
                    logger.error( "Unable to check if stream queue {} has next element", streamId, e );
                    return false;
                } finally {
                    lock.unlock();
                }
            } else {
                return position < length;
            }
        }

        private ListenableFuture<T> load() {
            return executor.submit( () -> stream.poll( 10, TimeUnit.MINUTES ) );
            //            return executor.submit( (Runnable) () -> {
            //                try {
            //                    nextAvailable = stream.poll( 10, TimeUnit.MINUTES );
            //                } catch ( InterruptedException e ) {
            //                    logger.error( "Unable to retrieve items from hazelcast stream.", e );
            //                }
            //            } );
        }

        @Override
        public T next() {
            T next = null;
            try {
                next = fNext.get();
            } catch ( InterruptedException | ExecutionException e ) {
                logger.error( "Unable to retrieve next element from stream queue {}", streamId, e );
            }

            if ( next == null ) {
                streamLock.unlock();
                streamLock.destroy();
                throw new NoSuchElementException( "The iterator backed by queue " + streamId + " is out of elements" );
            } else {
                try {
                    lock.lock();
                    fNext = load();
                    ++position;
                } finally {
                    lock.unlock();
                }
                return next;
            }
        }
    }
}
