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

import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.openlattice.graph.BooleanClauseVisitor;
import com.openlattice.graph.query.AbstractBooleanClauses.And;
import com.openlattice.graph.query.AbstractBooleanClauses.Or;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;

/**
 * Interface for
 */
@JsonTypeInfo( use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class" )
public interface BooleanClauses {
    int getId();

    EdmPrimitiveTypeKind getDatatype();

    Set<BooleanClauses> getChildClauses();

    default And and( BooleanClauses... queries ) {
        final Set<BooleanClauses> newQueries = new HashSet<>( queries.length + 1 );
        final List<BooleanClauses> toAnd = asList( queries );
        checkState( toAnd.stream().allMatch( eq -> eq.getDatatype().equals( eq.getDatatype() ) ),
                "Entity types match to perform boolean query!" );
        newQueries.add( this );
        newQueries.addAll( toAnd );
        return new And( getId(), getDatatype(), newQueries );
    }

    default Or or( BooleanClauses... queries ) {
        final Set<BooleanClauses> newQueries = new HashSet<>( queries.length + 1 );
        final List<BooleanClauses> toOr = asList( queries );
        checkState( toOr.stream().allMatch( eq -> eq.getDatatype().equals( eq.getDatatype() ) ),
                "Entity types match to perform boolean query!" );
        newQueries.add( this );
        newQueries.addAll( toOr );
        return new Or( getId(), getDatatype(), newQueries );
    }
}
