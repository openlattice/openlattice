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

import com.openlattice.data.integration.*;

import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.openlattice.edm.type.PropertyType;
import retrofit2.http.*;

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

    String ENTITY_SET         = "set";
    String ASSOCIATION        = "association";
    String ENTITY_KEY_IDS     = "entityKeyIds";
    String EDGES              = "edges";
    String POSTGRES_DATA_SINK = "postgresDataSink";
    String S3_DATA_SINK       = "s3DataSink";
    String PROPERTY_TYPES     = "propertyTypes";

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
     *                     UUID, Object &gt; details of entity) pairing enriched with source/destination EntityKeys
     */
    @POST( BASE + "/" + ASSOCIATION + "/" + SET_ID_PATH )
    IntegrationResults integrateAssociations(
            @Body Set<Association> associations,
            @Query( DETAILED_RESULTS ) boolean detailedResults );

    @POST( BASE )
    IntegrationResults integrateEntityAndAssociationData(
            @Body BulkDataCreation data,
            @Query( DETAILED_RESULTS ) boolean detailedResults );

    @POST( BASE + "/" + POSTGRES_DATA_SINK + "/" )
    IntegrationResults sinkToPostgres(
            @Body Set<EntityData> data
    );

    @POST( BASE + "/" + S3_DATA_SINK )
    Set<URL> generatePresignedUrls(
            @Body Set<S3EntityData> data
    );

    @POST( BASE + "/" + ENTITY_KEY_IDS )
    Map<UUID, Map<String, UUID>> getEntityKeyIds(
            @Body Set<EntityKey> entityKeys
    );

    @PUT( BASE + "/" + EDGES )
    int createEdges(
            @Body Set<DataEdgeKey> edges
    );

    @GET( BASE + "/" + PROPERTY_TYPES + "/" + SET_ID_PATH )
    Map<UUID, PropertyType> getPropertyTypesForEntitySet(
            @Path( ENTITY_SET_ID ) UUID entitySetId
    );
}
