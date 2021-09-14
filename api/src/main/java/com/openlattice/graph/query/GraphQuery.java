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
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class GraphQuery {
    private final List<EntityQuery>        entityQueries;
    private final List<AssociationIndexes> associationQueries;

    @JsonCreator
    public GraphQuery(
            @JsonProperty( SerializationConstants.ENTITY_QUERIES ) List<EntityQuery> entityQueries,
            @JsonProperty( SerializationConstants.ASSOCIATION_QUERIES ) List<AssociationIndexes> associationQueries ) {
        this.entityQueries = entityQueries;
        this.associationQueries = associationQueries;
    }

    @JsonProperty( SerializationConstants.ENTITY_QUERIES )
    public List<EntityQuery> getEntityQueries() {
        return entityQueries;
    }

    @JsonProperty( SerializationConstants.ASSOCIATION_QUERIES )
    public List<AssociationIndexes> getAssociationQueries() {
        return associationQueries;
    }

    public static class AssociationIndexes implements EdgeQuery {
        private final int  id;
        private final UUID associationEntityTypeId;
        private final int  srcIndex;
        private final int  dstIndex;
        private final int  edgeIndex;

        @JsonCreator
        public AssociationIndexes(
                @JsonProperty( SerializationConstants.ID_FIELD ) int id,
                @JsonProperty( SerializationConstants.ASSOCIATION_TYPE_ID ) UUID associationEntityTypeId,
                @JsonProperty( SerializationConstants.SRC ) int srcIndex,
                @JsonProperty( SerializationConstants.DST ) int dstIndex,
                @JsonProperty( SerializationConstants.EDGE ) int edgeIndex ) {
            this.associationEntityTypeId = associationEntityTypeId;
            this.id = id;
            this.srcIndex = srcIndex;
            this.dstIndex = dstIndex;
            this.edgeIndex = edgeIndex;
        }

        @Override public int getId() {
            return id;
        }

        public int getSrcIndex() {
            return srcIndex;
        }

        public int getDstIndex() {
            return dstIndex;
        }

        public int getEdgeIndex() {
            return edgeIndex;
        }

        @Override public UUID getAssociationEntityTypeId() {
            return null;
        }

        @Override public Set<EdgeQuery> getChildQueries() {
            return ImmutableSet.of();
        }
    }
}
