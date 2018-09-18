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

package com.openlattice.analysis;

import com.openlattice.analysis.requests.NeighborType;
import com.openlattice.analysis.requests.FilteredRanking;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.openlattice.data.requests.FileType;
import com.google.common.collect.SetMultimap;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import retrofit2.http.*;

public interface AnalysisApi {

    String SERVICE            = "/datastore";
    String CONTROLLER         = "/analysis";
    String BASE               = SERVICE + CONTROLLER;

    String TYPES_PATH         = "/types";

    String FILE_TYPE          = "fileType";

    String ENTITY_SET_ID      = "entitySetId";
    String NUM_RESULTS        = "numResults";
    String ENTITY_SET_ID_PATH = "/{" + ENTITY_SET_ID + "}";
    String NUM_RESULTS_PATH   = "/{" + NUM_RESULTS + "}";

    /**
     * Returns the top rows in the entity set, ordered by the sum of the number of items in each of the property types
     * provided Entity ids and count are included for each row.
     * 
     * @param entitySetId The id of the entity set to sort and return results for
     * @param numResults The number of results to return
     * @param topUtilizerDetails A list of objects each specifying an association type and at least one neighbor types
     *            to include in a top utilizers search, as well as an indication of the directionality. The results will
     *            be ordered by the total number of values across all property types provided (i.e. the sum of all the
     *            property types' value array size).
     * @return
     */
    @POST( BASE + ENTITY_SET_ID_PATH + NUM_RESULTS_PATH )
    Iterable<Map<String, Object>> getTopUtilizers(
            @Path( ENTITY_SET_ID ) UUID entitySetId,
            @Path( NUM_RESULTS ) int numResults,
            @Body List<FilteredRanking> topUtilizerDetails,
            @Query( FILE_TYPE ) FileType fileType );

    @GET( BASE + ENTITY_SET_ID_PATH + TYPES_PATH )
    Iterable<NeighborType> getNeighborTypes( @Path( ENTITY_SET_ID ) UUID entitySetId );
}
