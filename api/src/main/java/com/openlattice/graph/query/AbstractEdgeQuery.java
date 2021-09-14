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

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;
import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public abstract class AbstractEdgeQuery implements EdgeQuery {
    private final int            id;
    private final UUID           associationEntityTypeId;
    private final Set<EdgeQuery> childQueries;

    public AbstractEdgeQuery( int id, UUID associationEntityTypeId, Set<EdgeQuery> childQueries ) {
        this.id = id;
        this.associationEntityTypeId = associationEntityTypeId;
        this.childQueries = childQueries;
    }

    @Override
    @JsonProperty( SerializationConstants.ID_FIELD )
    public int getId() {
        return id;
    }

    @Override
    @JsonProperty( SerializationConstants.ASSOCIATION_ENTITY_TYPE )
    public UUID getAssociationEntityTypeId() {
        return associationEntityTypeId;
    }

    public Set<EdgeQuery> getChildQueries() {
        return childQueries;
    }

    public static class And extends AbstractEdgeQuery {
        public And( int id, UUID associationEntityTypeId, Set<EdgeQuery> childQueries ) {
            super( id, associationEntityTypeId, childQueries );
            checkArgument( !childQueries.isEmpty(), "Child queries cannot be empty for an AND query" );
        }
    }

    public static class Or extends AbstractEdgeQuery {
        public Or( int id, UUID associationEntityTypeId, Set<EdgeQuery> childQueries ) {
            super( id, associationEntityTypeId, childQueries );
            checkArgument( !childQueries.isEmpty(), "Child queries cannot be empty for an OR query" );
        }
    }
}
