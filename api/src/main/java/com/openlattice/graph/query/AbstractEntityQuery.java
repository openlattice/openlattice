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
import com.openlattice.client.serialization.SerializationConstants;
import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

public abstract class AbstractEntityQuery implements EntityQuery {
    private final int              id;
    private final UUID             entityTypeId;
    private final Set<EntityQuery> childQueries;

    public AbstractEntityQuery( int id, UUID entityTypeId, Set<EntityQuery> childQueries ) {
        this.id = id;
        this.entityTypeId = entityTypeId;
        this.childQueries = childQueries;
    }

    @Override
    @JsonProperty( SerializationConstants.ID_FIELD )
    public int getId() {
        return id;
    }

    @JsonProperty( SerializationConstants.ENTITY_TYPE_ID )
    @Override public UUID getEntityTypeId() {
        return entityTypeId;
    }

    public Set<EntityQuery> getChildQueries() {
        return childQueries;
    }

    public static class And extends AbstractEntityQuery {
        @JsonCreator
        public And(
                @JsonProperty( SerializationConstants.ID_FIELD ) int id,
                @JsonProperty( SerializationConstants.ENTITY_TYPE_ID ) UUID entityTypeId,
                @JsonProperty( SerializationConstants.ENTITY_QUERIES ) Set<EntityQuery> childQueries ) {
            super( id, entityTypeId, childQueries );
        }
    }

    public static class Or extends AbstractEntityQuery {
        @JsonCreator
        public Or(
                @JsonProperty( SerializationConstants.ID_FIELD ) int id,
                @JsonProperty( SerializationConstants.ENTITY_TYPE_ID ) UUID entityTypeId,
                @JsonProperty( SerializationConstants.ENTITY_QUERIES ) Set<EntityQuery> childQueries ) {
            super( id, entityTypeId, childQueries );
        }
    }

}
