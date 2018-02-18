/*
 * Copyright (C) 2018. OpenLattice, Inc.
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

package com.openlattice.datastore.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;

public class FuturesAdapter {
    private FuturesAdapter() {}

    /*
     * Based on Guava Futures.addCallback Impl
     */
    public static <T> ListenableFuture<T> wrap( ICompletableFuture<T> hzFuture ) {
        return new ICompletableFutureWrapper<T>( hzFuture );
    }

    public static class ICompletableFutureWrapper<T> implements ListenableFuture<T> {
        private static final Logger         logger = LoggerFactory.getLogger( ICompletableFutureWrapper.class );

        private final ICompletableFuture<T> future;

        ICompletableFutureWrapper( ICompletableFuture<T> future ) {
            this.future = future;
        }

        @Override
        public boolean cancel( boolean mayInterruptIfRunning ) {
            return future.cancel( mayInterruptIfRunning );
        }

        @Override
        public boolean isCancelled() {
            return future.isCancelled();
        }

        @Override
        public boolean isDone() {
            return future.isDone();
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            return future.get();
        }

        @Override
        public T get( long timeout, TimeUnit unit ) throws InterruptedException, ExecutionException, TimeoutException {
            return future.get( timeout, unit );
        }

        @Override
        public void addListener( Runnable listener, Executor executor ) {
            future.andThen( new ExecutionCallback<T>() {

                @Override
                public void onResponse( T response ) {
                    listener.run();
                }

                @Override
                public void onFailure( Throwable t ) {
                    logger.error( "Failed to complete ICompletableFuture: {}", t.getLocalizedMessage() );
                }
            }, executor );
        }
    }
}
