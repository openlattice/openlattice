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

package com.openlattice.datasource;

import retrofit2.http.*;

import java.util.UUID;

/**
 * This API used for managing
 */
public interface DatasourceApi {
    /*
     * These determine the service routing for the LB
     */
    String SERVICE    = "/datastore";
    String CONTROLLER = "/datasource";
    String BASE       = SERVICE + CONTROLLER;

    String ID   = "id";
    String SYNC = "sync";

    @POST( BASE )
    UUID createOrUpdateDatasource( @Body ApiDatasource datasource );

    @GET( BASE + "/{" + ID + "}" )
    ApiDatasource getDatasource( @Path( ID ) UUID datasourceId );

    @DELETE( BASE + "/{" + ID + "}" )
    Void deleteDatasource( @Path( ID ) UUID datasourceId );

}
