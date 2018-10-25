/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

package com.openlattice.graph;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.SetMultimap;
import com.openlattice.graph.query.GraphQuery;
import com.openlattice.graph.query.GraphQueryState;
import com.openlattice.graph.query.GraphQueryState.Option;
import java.util.Set;
import java.util.UUID;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.Path;

/**
 * Used for performing graph queries on the backend.
 */
public interface GraphApi {
    String CONTROLLER = "/graph";
    String ID      = "id";
    String ID_PATH = "/{" + ID + "}";
    //@formatter:on
    String QUERY  = "/query";
    String RESULT = "/result";
    //@formatter:off
    String SERVICE    = "/datastore";
    String BASE       = SERVICE + CONTROLLER;

    @POST( BASE + QUERY )
    GraphQueryState submit( SimpleGraphQuery query );

    @POST( BASE + QUERY + ID_PATH )
    GraphQueryState getQueryState( @Path( ID ) UUID queryId, @Body Set<Option> options );

    /**
     * Retrieves the graph query state with any additional options. Equivalent to {@code getQueryState(queryId, Collections.EMPTY_SET) }
     */
    @GET( BASE + QUERY + ID_PATH )
    GraphQueryState getQueryState( @Path( ID ) UUID queryId );

    @GET( BASE + QUERY + ID_PATH + RESULT )
    SubGraph getResults( UUID queryId );

    /**
     *
     * @param ops
     * @return
     */
    ListMultimap<UUID, SetMultimap<UUID, SetMultimap<UUID, Object>>> graphQuery( GraphQuery ops ); // Entity Key Id -> Property Type Id -> Entity Set Id
    //That is a list of entities along with properties annotated with which entity set they came from.

}
