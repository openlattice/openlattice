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

package com.openlattice.edm.type;

import java.util.LinkedHashSet;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AssociationDetails {
    private LinkedHashSet<EntityType> srcEntityTypes;
    private LinkedHashSet<EntityType> dstEntityTypes;
    private boolean                   bidirectional;

    public AssociationDetails(
            @JsonProperty( SerializationConstants.SRC ) LinkedHashSet<EntityType> srcEntityTypes,
            @JsonProperty( SerializationConstants.DST ) LinkedHashSet<EntityType> dstEntityTypes,
            @JsonProperty( SerializationConstants.BIDIRECTIONAL ) boolean bidirectional ) {
        this.srcEntityTypes = srcEntityTypes;
        this.dstEntityTypes = dstEntityTypes;
        this.bidirectional = bidirectional;
    }

    @JsonProperty( SerializationConstants.SRC )
    public LinkedHashSet<EntityType> getSrcEntityTypes() {
        return srcEntityTypes;
    }

    @JsonProperty( SerializationConstants.DST )
    public LinkedHashSet<EntityType> getDstEntityTypes() {
        return dstEntityTypes;
    }

    @JsonProperty( SerializationConstants.BIDIRECTIONAL )
    public boolean isBidirectional() {
        return bidirectional;
    }

}
