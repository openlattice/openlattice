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
import static com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;
import java.util.Set;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

public abstract class AbstractBooleanClauses implements BooleanClauses {
    private final int                  id;
    private final EdmPrimitiveTypeKind datatype;
    private final Set<BooleanClauses>  childClauses;

    public AbstractBooleanClauses( int id, EdmPrimitiveTypeKind datatype, Set<BooleanClauses> childClauses ) {
        this.id = id;
        this.datatype = datatype;
        this.childClauses = childClauses;
    }

    @Override
    @JsonProperty( SerializationConstants.ID_FIELD )
    public int getId() {
        return id;
    }

    @Override
    @JsonProperty( SerializationConstants.DATATYPE_FIELD )
    public EdmPrimitiveTypeKind getDatatype() {
        return datatype;
    }

    @Override
    @JsonProperty( SerializationConstants.CHILD_QUERIES )
    public Set<BooleanClauses> getChildClauses() {
        return childClauses;
    }

    public static class And extends AbstractBooleanClauses {
        @JsonCreator
        public And(
                @JsonProperty( SerializationConstants.ID_FIELD ) int id,
                @JsonProperty( SerializationConstants.ENTITY_TYPE_ID ) EdmPrimitiveTypeKind datatype,
                @JsonProperty( SerializationConstants.CHILD_QUERIES ) Set<BooleanClauses> childClauses ) {
            super( id, datatype, childClauses );
            checkArgument( !childClauses.isEmpty(), "Child clauses cannot be empty for an AND query");
        }
    }

    public static class Or extends AbstractBooleanClauses {
        @JsonCreator
        public Or(
                @JsonProperty( SerializationConstants.ID_FIELD ) int id,
                @JsonProperty( SerializationConstants.ENTITY_TYPE_ID ) EdmPrimitiveTypeKind datatype,
                @JsonProperty( SerializationConstants.CHILD_QUERIES ) Set<BooleanClauses> childClauses ) {
            super( id, datatype, childClauses );
            checkArgument( !childClauses.isEmpty(), "Child clauses cannot be empty for an Or query");
        }
    }

}
