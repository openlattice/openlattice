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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class IntegrationResults {
    private final int entityCount;
    private final int associationCount;

    private final Optional<Map<UUID,Map<String, UUID>>> entitySetEntityKeyIds;
    private final Optional<Map<UUID,Map<String, UUID>>> associationEntityKeyIds;

    @JsonCreator
    public IntegrationResults(
            @JsonProperty( SerializationConstants.ENTITY_COUNT) int entityCount,
            @JsonProperty( SerializationConstants.ASSOCIATION_COUNT) int associationCount,
            @JsonProperty(SerializationConstants.ASSOCIATIONS_ENTITY_KEY_MAPPINGS ) Optional<Map<UUID, Map<String, UUID>>> entitySetEntityKeyIds,
            @JsonProperty(SerializationConstants.ENTITY_SETS_ENTITY_KEY_MAPPINGS) Optional<Map<UUID, Map<String, UUID>>> associationEntityKeyIds ) {
        this.entityCount = entityCount;
        this.associationCount = associationCount;
        this.entitySetEntityKeyIds = entitySetEntityKeyIds;
        this.associationEntityKeyIds = associationEntityKeyIds;
    }
}
