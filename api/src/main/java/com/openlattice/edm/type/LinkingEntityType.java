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

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;

import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Deprecated
public class LinkingEntityType {
    private final EntityType linkingEntityType;
    private final Set<UUID>  linkedEntityTypes;
    private final boolean    deidentified;

    @JsonCreator
    public LinkingEntityType(
            @JsonProperty( SerializationConstants.ENTITY_TYPE ) EntityType linkingEntityType,
            @JsonProperty( SerializationConstants.ENTITY_TYPE_IDS_FIELD ) Set<UUID> linkedEntityTypes,
            @JsonProperty( SerializationConstants.DEIDENTIFIED ) Optional<Boolean> deidentified ) {
        this.linkingEntityType = linkingEntityType;
        this.linkedEntityTypes = linkedEntityTypes;
        this.deidentified = deidentified.or( true );
    }

    @JsonProperty( SerializationConstants.ENTITY_TYPE )
    public EntityType getLinkingEntityType() {
        return linkingEntityType;
    }

    @JsonProperty( SerializationConstants.ENTITY_TYPE_IDS_FIELD )
    public Set<UUID> getLinkedEntityTypes() {
        return linkedEntityTypes;
    }

    @JsonProperty( SerializationConstants.DEIDENTIFIED )
    public boolean isDeidentified() {
        return deidentified;
    }

}
