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

package com.openlattice.edm.internal;

import java.util.UUID;

public final class DatastoreConstants {

    private DatastoreConstants() {}

    /**
     * This property is deprecated and should be dynamically configured.
     */
    @Deprecated
    public static final String KEYSPACE                = "sparks";

    // TABLES
    public static final String ENTITY_SETS_TABLE       = "entity_sets";
    public static final String CONTAINERS_TABLE        = "containers";
    public static final String COUNT_FIELD             = "count";
    public static final String ENTITY_TYPES_TABLE      = "entity_types";
    public static final String PROPERTY_TYPES_TABLE    = "property_types";

    // PARAMETERS
    public static final String APPLIED_FIELD           = "[applied]";

    // DEFAULT
    public static final UUID   DEFAULT_ORGANIZATION_ID = new UUID( 0, 0 );

}
