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
 */

package com.openlattice.linking.util;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.openlattice.linking.LinkingEntityKey;
import com.google.common.base.Preconditions;

public class UnorderedPair<T> {
    private final Set<T> backingCollection = new HashSet<T>( 2 );

    public UnorderedPair( T a, T b ) {
        backingCollection.add( a );
        backingCollection.add( b );
    }
    
    public UnorderedPair( Set<T> pair ){
        Preconditions.checkArgument( pair.size() <= 2, "There are more than two elements in the set." );
        backingCollection.addAll( pair );
    }

    public Set<T> getBackingCollection() {
        return backingCollection;
    }

    public List<T> getAsList() {
        return backingCollection.stream().collect( Collectors.toList() );
    }

    @Override
    public int hashCode() {
        return backingCollection.hashCode();
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( !( obj instanceof UnorderedPair<?> ) ) return false;
        return backingCollection.equals( ( (UnorderedPair<?>) obj ).backingCollection );
    }

    @Override
    public String toString() {
        return "UnorderedPair backed by " + backingCollection;
    }

}
