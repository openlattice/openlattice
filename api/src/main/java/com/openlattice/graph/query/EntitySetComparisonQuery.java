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
import java.util.Optional;
import java.util.UUID;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntitySetComparisonQuery extends EntitySetQuery {
    private final FullQualifiedName fqn;
    private final Object            value;
    private final ComparisonOp      comparisonOp;

    @JsonCreator
    public EntitySetComparisonQuery(
            Optional<UUID> entitySetId,
            UUID entityTypeId,
            FullQualifiedName fqn,
            Object value,
            ComparisonOp comparisonOp ) {
        super( entitySetId, entityTypeId );
        this.fqn = fqn;
        this.value = value;
        this.comparisonOp = comparisonOp;
    }

    public ComparisonOp getComparisonOp() {
        return comparisonOp;
    }

    public FullQualifiedName getFqn() {
        return fqn;
    }

    public Object getValue() {
        return value;
    }

    enum ComparisonOp {
        EQUAL,
        LT,
        LTE,
        GT,
        GTE;

        String getComparisionString() {
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
