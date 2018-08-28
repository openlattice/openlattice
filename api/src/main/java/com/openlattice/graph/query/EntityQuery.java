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
import com.openlattice.graph.EntityQueryVisitor;
import com.openlattice.graph.query.AbstractEntityQuery.And;
import com.openlattice.graph.query.AbstractEntityQuery.Or;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Interface for
 */
@JsonTypeInfo( use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class" )
public interface EntityQuery {
    int getId();

    UUID getEntityTypeId();

    Set<EntityQuery> getChildQueries();

    default And and( EntityQuery... queries ) {
        final Set<EntityQuery> newQueries = new HashSet<>( queries.length + 1 );
        final List<EntityQuery> toAnd = asList( queries );
        checkState( toAnd.stream().allMatch( eq -> eq.getEntityTypeId().equals( eq.getEntityTypeId() ) ),
                "Entity types match to perform boolean query!" );
        newQueries.add( this );
        newQueries.addAll( toAnd );
        return new And( getId(), getEntityTypeId(), newQueries );
    }

    default Or or( EntityQuery... queries ) {
        final Set<EntityQuery> newQueries = new HashSet<>( queries.length + 1 );
        final List<EntityQuery> toOr = asList( queries );
        checkState( toOr.stream().allMatch( eq -> eq.getEntityTypeId().equals( eq.getEntityTypeId() ) ),
                "Entity types match to perform boolean query!" );
        newQueries.add( this );
        newQueries.addAll( toOr );
        return new Or( getId(), getEntityTypeId(), newQueries );
    }
}
