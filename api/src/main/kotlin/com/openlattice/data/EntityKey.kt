/*
 * Copyright (C) 2020. OpenLattice, Inc.
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

package com.openlattice.data

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import java.util.*

/**
 * Uniquely identifies a version of an entity in an entity set.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class EntityKey(
        @JsonProperty(SerializationConstants.ENTITY_SET_ID) val entitySetId: UUID,
        @JsonProperty(SerializationConstants.ENTITY_ID) val entityId: String
) : Comparable<EntityKey>{

    override fun compareTo(other: EntityKey): Int {
        var result = entitySetId.compareTo(other.entitySetId)
        if (result == 0) {
            result = entityId.compareTo(other.entityId)
        }
        return result
    }
}