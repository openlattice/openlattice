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

import com.openlattice.EntityKeyGenerationBundle;
import com.openlattice.data.integration.S3EntityData;
import java.util.Map;
import retrofit2.http.Body;
import retrofit2.http.POST;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public interface DataIntegrationApi {

    /*
     * These determine the service routing for the LB
     */

    // @formatter:off
    String SERVICE               = "/datastore";
    String CONTROLLER            = "/integration";
    String BASE                  = SERVICE + CONTROLLER;
    // @formatter:on

    String ENTITY_KEY_IDS = "entityKeyIds";
    String ENTITY_KEYS = "entityKeys";
    /**
     * To discuss paths later; perhaps batch this with EdmApi paths
     */

    String S3 = "s3";

    @POST( BASE + "/" + S3 )
    List<String> generatePresignedUrls( @Body Collection<S3EntityData> data );

    @POST( BASE + "/" + ENTITY_KEY_IDS )
    List<UUID> getEntityKeyIds( @Body Set<EntityKey> entityKeys );

    @POST( BASE + "/" + ENTITY_KEYS )
    Map<UUID,EntityKey> generateEntityKeys( @Body EntityKeyGenerationBundle bundle );
}
