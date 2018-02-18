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

package com.openlattice.edm.set;

import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.edm.EntitySet;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class LinkingEntitySet {
    private final EntitySet            entitySet;
    private final Set<Map<UUID, UUID>> linkingProperties;

    @JsonCreator
    public LinkingEntitySet(
            @JsonProperty( SerializationConstants.ENTITY_SET_FIELD ) EntitySet entitySet,
            @JsonProperty( SerializationConstants.LINKING_PROPERTIES_FIELD ) Set<Map<UUID, UUID>> linkingProperties ) {
        this.entitySet = entitySet;
        this.linkingProperties = linkingProperties;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_FIELD )
    public EntitySet getEntitySet() {
        return entitySet;
    }

    @JsonProperty( SerializationConstants.LINKING_PROPERTIES_FIELD )
    public Set<Map<UUID, UUID>> getLinkingProperties() {
        return linkingProperties;
    }
}
