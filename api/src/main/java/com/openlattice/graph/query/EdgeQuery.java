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
import com.openlattice.graph.query.AbstractEdgeQuery.And;
import com.openlattice.graph.query.AbstractEdgeQuery.Or;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@JsonTypeInfo( use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class" )
public interface EdgeQuery {
    int getId();

    UUID getAssociationEntityTypeId();

    Set<EdgeQuery> getChildQueries();

    default And and( EdgeQuery... queries ) {
        final Set<EdgeQuery> newQueries = new HashSet<>( queries.length + 1 );
        final List<EdgeQuery> toAnd = asList( queries );
        checkState( toAnd
                        .stream()
                        .allMatch( eq -> eq.getAssociationEntityTypeId().equals( eq.getAssociationEntityTypeId() ) ),
                "Association entity types must match to combine in boolean fashion." );
        newQueries.add( this );
        newQueries.addAll( toAnd );
        return new And( getId(), getAssociationEntityTypeId(), newQueries );
    }

    default Or or( EdgeQuery... queries ) {
        final Set<EdgeQuery> newQueries = new HashSet<>( queries.length + 1 );
        final List<EdgeQuery> toOr = asList( queries );
        checkState( toOr
                        .stream()
                        .allMatch( eq -> eq.getAssociationEntityTypeId().equals( eq.getAssociationEntityTypeId() ) ),
                "Association entity types must match to combine in boolean fashion." );
        newQueries.add( this );
        newQueries.addAll( toOr );
        return new Or( getId(), getAssociationEntityTypeId(), newQueries );
    }
}
