

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

package com.openlattice.authorization;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;

public class AceFuture implements ListenableFuture<Ace> {
    private static final Logger                              logger = LoggerFactory.getLogger( AceFuture.class );
    private final Principal                                      principal;
    private final ICompletableFuture<DelegatedPermissionEnumSet> futurePermissions;

    public AceFuture( Principal principal, ICompletableFuture<DelegatedPermissionEnumSet> futurePermissions ) {
        this.principal = principal;
        this.futurePermissions = futurePermissions;
    }

    @Override
    public boolean cancel( boolean mayInterruptIfRunning ) {
        return futurePermissions.cancel( mayInterruptIfRunning );
    }

    @Override
    public boolean isCancelled() {
        return futurePermissions.isCancelled();
    }

    @Override
    public boolean isDone() {
        return futurePermissions.isDone();
    }

    @Override
    public Ace get() throws InterruptedException, ExecutionException {
        return new Ace( principal, futurePermissions.get() );
    }

    public Ace getUninterruptibly() {
        try {
            return get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Failed to get Ace", e );
            return null;
        }
    }

    @Override
    public Ace get( long timeout, TimeUnit unit ) throws InterruptedException, ExecutionException, TimeoutException {
        return new Ace( principal, futurePermissions.get( timeout, unit ) );
    }

    @Override
    public void addListener( Runnable listener, Executor executor ) {
        futurePermissions.andThen( new ExecutionCallback<DelegatedPermissionEnumSet>() {

            @Override
            public void onResponse( DelegatedPermissionEnumSet response ) {
                listener.run();
            }

            @Override
            public void onFailure( Throwable t ) {
                logger.error( "Unable to retrieve Ace.", t );
            }
        }, executor );
    }
}
