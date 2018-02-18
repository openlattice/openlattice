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

package com.openlattice.edm;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;
import java.util.UUID;

/**
 * Used for specifying a selection of EdmTypes
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EdmSelection {
    private final Set<UUID> propertyTypes;
    private final Set<UUID> entityTypes;

    @JsonCreator
    public EdmSelection(
            @JsonProperty( SerializationConstants.PROPERTY_TYPES ) Set<UUID> propertyTypes,
            @JsonProperty( SerializationConstants.ENTITY_TYPES ) Set<UUID> entityTypes ) {
        this.propertyTypes = propertyTypes;
        this.entityTypes = entityTypes;
    }

    @JsonProperty( SerializationConstants.PROPERTY_TYPES )
    public Set<UUID> getPropertyTypes() {
        return propertyTypes;
    }

    @JsonProperty( SerializationConstants.ENTITY_TYPES )
    public Set<UUID> getEntityTypes() {
        return entityTypes;
    }
}
