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

package com.openlattice.data;

import com.google.common.collect.SetMultimap;
import com.openlattice.data.integration.Association;
import com.openlattice.data.integration.BulkDataCreation;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.openlattice.data.integration.DataSinkObject;
import retrofit2.http.Body;
import retrofit2.http.POST;
import retrofit2.http.Path;
import retrofit2.http.Query;

public interface DataIntegrationApi {
    /*
     * These determine the service routing for the LB
     */
    String SERVICE    = "/datastore";
    String CONTROLLER = "/integration";
    String BASE       = SERVICE + CONTROLLER;

    /**
     * To discuss paths later; perhaps batch this with EdmApi paths
     */

    String ENTITY_SET  = "set";
    String ASSOCIATION = "association";
    String DATA_SINK = "dataSink";

    String ENTITY_SET_ID    = "setId";
    String ENTITY_KEY_ID    = "entityKeyId";
    String PROPERTY_TYPE_ID = "propertyTypeId";

    String COUNT            = "count";
    String DETAILED_RESULTS = "detailedResults";
    String UPDATE           = "update";

    String ENTITY_KEY_ID_PATH    = "{" + ENTITY_KEY_ID + "}";
    String SET_ID_PATH           = "{" + ENTITY_SET_ID + "}";
    String PROPERTY_TYPE_ID_PATH = "{" + PROPERTY_TYPE_ID + "}";

    @POST( BASE + "/" + ENTITY_SET + "/" + SET_ID_PATH )
    IntegrationResults integrateEntities(
            @Path( ENTITY_SET_ID ) UUID entitySetId,
            @Query( DETAILED_RESULTS ) boolean detailedResults,
            @Body Map<String, Map<UUID, Set<Object>>> entities );

    /**
     * Creates a new set of associations.
     *
     * @param associations Set of associations to create. An association is the usual (String entityId, SetMultimap &lt;
     * UUID, Object &gt; details of entity) pairing enriched with source/destination EntityKeys
     */
    @POST( BASE + "/" + ASSOCIATION + "/" + SET_ID_PATH )
    IntegrationResults integrateAssociations(
            @Body Set<Association> associations,
            @Query( DETAILED_RESULTS ) boolean detailedResults );

    @POST( BASE )
    IntegrationResults integrateEntityAndAssociationData(
            @Body BulkDataCreation data,
            @Query( DETAILED_RESULTS ) boolean detailedResults );

    @POST (BASE + "/" + DATA_SINK)
    IntegrationResults sinkData(@Body DataSinkObject data);

    @POST (BASE + "/" + ENTITY_KEY_ID)
    Map<EntityKey, UUID> getEntityKeyIds(
            @Body Set<EntityKey> entityKeys
    );

}
