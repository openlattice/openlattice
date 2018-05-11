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

import static java.util.Arrays.asList;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public interface Query<T> {
    Set<Query<T>> getChildQueries();

    default <O> O visit( Function<T, O> map, Function<Set<O>, O> reduce ) {
        return reduce
                .apply( getChildQueries().stream().map( query -> query.visit( map, reduce ) ).collect( Collectors.toSet() ) );
    }

    default And and( Supplier<Integer> id, Query... queries ) {
        Set<Query> newQueries = new HashSet<>( queries.length + 1 );
        newQueries.add( this );
        newQueries.addAll( asList( queries ) );
        return new And( newQueries );
    }

    default Or or( Query... queries ) {
        Set<Query> newQueries = new HashSet<>( queries.length + 1 );
        newQueries.add( this );
        newQueries.addAll( asList( queries ));
        return new Or( newQueries );
    }

}
