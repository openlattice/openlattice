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

package com.openlattice.sync;

import java.util.UUID;

import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.Path;

public interface SyncApi {

    String SERVICE            = "/datastore";
    String CONTROLLER         = "/sync";
    String BASE               = SERVICE + CONTROLLER;

    String ENTITY_SET_ID      = "entitySetId";
    String SYNC_ID            = "syncId";

    String ENTITY_SET_ID_PATH = "/{" + ENTITY_SET_ID + "}";
    String SYNC_ID_PATH       = "/{" + SYNC_ID + "}";
    String CURRENT            = "/current";
    String LATEST             = "/latest";

    /**
     * Generates a new sync id for an entity set. If the entity set has no current sync id, the new one is set as
     * current.
     * 
     * @param entitySetId The id of the entity set to generate a new sync id for.
     * @return A new time-uuid generated in data source api.
     */
    @GET( BASE + ENTITY_SET_ID_PATH )
    UUID acquireSyncId(
            @Path( ENTITY_SET_ID ) UUID entitySetId );

    /**
     * Retrieves the current sync id for the given entity set
     * 
     * @param entitySetId The id of the entity set to load the current sync id for.
     * @return The current time-uuid for the entity set.
     */
    @GET( BASE + ENTITY_SET_ID_PATH + CURRENT )
    UUID getCurrentSyncId( @Path( ENTITY_SET_ID ) UUID entitySetId );

    /**
     * Sets the current syncId for the entity set to the specified syncId. By default, reads for the entity set will
     * come from the current syncId, and writes will be written to the current sync, unless otherwise specified.
     * 
     * @param entitySetId The id of the entity set to set the current sync id for.
     * @param syncId the sync id that will be set as current
     */
    @POST( BASE + ENTITY_SET_ID_PATH + SYNC_ID_PATH )
    Void setCurrentSyncId( @Path( ENTITY_SET_ID ) UUID entitySetId, @Path( SYNC_ID ) UUID syncId );

    /**
     * Retrieves the most recently created sync id for the given entity set
     * 
     * @param entitySetId The id of the entity set to load the latest sync id for.
     * @return The latest time-uuid for the entity set.
     */
    @GET( BASE + ENTITY_SET_ID_PATH + LATEST )
    UUID getLatestSyncId( @Path( ENTITY_SET_ID ) UUID entitySetId );

}
