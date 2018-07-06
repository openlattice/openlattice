/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.graph.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.openlattice.client.serialization.SerializationConstants;
import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntityKeyIdQuery implements EntityQuery {
    private final UUID entitySetId;
    private final UUID entityKeyId;
    private final UUID entityTypeId;

    @JsonCreator
    public EntityKeyIdQuery(
            @JsonProperty( SerializationConstants.ENTITY_SET_ID ) UUID entitySetId,
            @JsonProperty( SerializationConstants.ID_FIELD ) UUID entityKeyId,
            @JsonProperty( SerializationConstants.ENTITY_TYPE_ID ) UUID entityTypeId ) {
        this.entitySetId = entitySetId;
        this.entityKeyId = entityKeyId;
        this.entityTypeId = entityTypeId;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_ID )
    public UUID getEntitySetId() {
        return entitySetId;
    }

    @JsonProperty( SerializationConstants.ID_FIELD )
    public UUID getEntityKeyId() {
        return entityKeyId;
    }

    @JsonProperty( SerializationConstants.ENTITY_TYPE_ID )
    @Override public UUID getEntityTypeId() {
        return entityTypeId;
    }

    @Override public Set<EntityQuery> getChildQueries() {
        return ImmutableSet.of();
    }
}
