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

package com.openlattice.linking;

import com.openlattice.data.DataApi;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import retrofit2.http.*;

/**
 * This API is used for creating and managing synthetic entity sets created by linking several entity sets together. The
 * entity sets created by this invoking this API are not actually instantiated until they are accessed via the
 * {@link DataApi}, which dynamically weaves them together based on the matching information.
 *
 */
public interface LinkingApi {
    String CONTROLLER       = "/linking";
    String SERVICE          = "/datastore";
    String BASE             = SERVICE + CONTROLLER;

    String SET              = "/set";

    String SET_ID           = "setId";
    String SET_ID_PATH      = "{" + SET_ID + "}";

    @POST( BASE  + SET )
    Integer addEntitySetsToLinkingEntitySets( @Body Map<UUID, Set<UUID>> entitySetIds );

    @HTTP( method = "DELETE", path = BASE + SET, hasBody = true)
    Integer removeEntitySetsFromLinkingEntitySets( @Body Map<UUID, Set<UUID>> entitySetIds );

    @PUT( BASE + SET + SET_ID_PATH )
    Integer addEntitySetsToLinkingEntitySet( @Path( SET_ID ) UUID linkingEntitySetId, @Body Set<UUID> entitySetIds );

    @HTTP( method = "DELETE", path =  BASE + SET + SET_ID_PATH, hasBody = true)
    Integer removeEntitySetsFromLinkingEntitySet( @Path( SET_ID ) UUID linkingEntitySetId, @Body Set<UUID> entitySetIds );

}
