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
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class ComparisonClause extends AbstractBooleanClauses {
    private final FullQualifiedName fqn;
    private final Object            value;
    private final ComparisonOp      comparisonOp;

    @JsonCreator
    public ComparisonClause(
            @JsonProperty( SerializationConstants.ID_FIELD ) int id,
            @JsonProperty( SerializationConstants.DATATYPE_FIELD ) EdmPrimitiveTypeKind datatype,
            @JsonProperty( SerializationConstants.FQN ) FullQualifiedName fqn,
            @JsonProperty( SerializationConstants.VALUE_FIELD ) Object value,
            @JsonProperty( SerializationConstants.COMPARISON ) ComparisonOp comparisonOp ) {
        super( id, datatype, ImmutableSet.of() );
        this.fqn = fqn;
        this.value = value;
        this.comparisonOp = comparisonOp;
    }

    @JsonProperty( SerializationConstants.COMPARISON )
    public ComparisonOp getComparisonOp() {
        return comparisonOp;
    }

    @JsonProperty( SerializationConstants.FQN )
    public FullQualifiedName getFqn() {
        return fqn;
    }

    @JsonProperty( SerializationConstants.VALUE_FIELD )
    public Object getValue() {
        return value;
    }

    public enum ComparisonOp {
        EQUAL,
        LT,
        LTE,
        GT,
        GTE;

        /**
         * These must all be reversed since ANY clause inverts form of logic.
         */
        public String getArrayComparisonString() {
            switch ( this ) {
                case EQUAL:
                    return "=";
                case LT:
                    return ">";
                case LTE:
                    return ">=";
                case GT:
                    return "<";
                case GTE:
                    return "<=";
            }
            return null;
        }

        public String getComparisonString() {
            switch ( this ) {
                case EQUAL:
                    return "=";
                case LT:
                    return "<";
                case LTE:
                    return "<=";
                case GT:
                    return ">";
                case GTE:
                    return ">=";
            }
            return null;
        }
    }
}
