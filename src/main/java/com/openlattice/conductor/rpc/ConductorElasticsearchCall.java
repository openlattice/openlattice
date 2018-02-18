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

package com.openlattice.conductor.rpc;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Function;

import com.google.common.base.Preconditions;

public class ConductorElasticsearchCall<T> implements Callable<T> {
    private final UUID                                   userId;
    private final Function<ConductorElasticsearchApi, T> f;
    private final ConductorElasticsearchApi              api;

    public ConductorElasticsearchCall(
            UUID userId,
            Function<ConductorElasticsearchApi, T> call,
            ConductorElasticsearchApi api ) {
        this.userId = Preconditions.checkNotNull( userId );
        this.f = Preconditions.checkNotNull( call );
        this.api = api;
    }

    public UUID getUserId() {
        return userId;
    }

    @Override
    public T call() throws Exception {
        return f.apply( api );
    }

    public static <T> ConductorElasticsearchCall<T> wrap( Function<ConductorElasticsearchApi, T> f ) {
        return new ConductorElasticsearchCall<T>( UUID.randomUUID(), f, null );
    }

    public Function<ConductorElasticsearchApi, T> getFunction() {
        return f;
    }
}
