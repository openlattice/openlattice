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

package com.openlattice.linking.requests;

import java.util.Set;
import java.util.UUID;

import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.edm.set.LinkingEntitySet;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LinkingRequest {
    private final LinkingEntitySet linkingEntitySet;
    private final Set<UUID>        resultPropertyTypeIds;

    @JsonCreator
    public LinkingRequest(
            @JsonProperty( SerializationConstants.LINKING_ENTITY_SET_FIELD ) LinkingEntitySet linkingEntitySet,
            @JsonProperty( SerializationConstants.PROPERTY_TYPE_ID_LIST ) Set<UUID> resultPropertyTypeIds ) {
        this.linkingEntitySet = linkingEntitySet;
        this.resultPropertyTypeIds = resultPropertyTypeIds;
    }

    @JsonProperty( SerializationConstants.LINKING_ENTITY_SET_FIELD )
    public LinkingEntitySet getLinkingEntitySet() {
        return linkingEntitySet;
    }

    @JsonProperty( SerializationConstants.PROPERTY_TYPE_ID_LIST )
    public Set<UUID> getResultPropertyTypeIds() {
        return resultPropertyTypeIds;
    }
}
