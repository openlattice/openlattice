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

package com.openlattice.graph.core.objects;

import com.google.common.base.Preconditions;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDList;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public class NeighborTripletSet implements Set<DelegatedUUIDList> {

    private final Set<DelegatedUUIDList> triplets;

    public NeighborTripletSet( Set<DelegatedUUIDList> triplets ) {
        this.triplets = triplets;
    }

    @Override public int size() {
        return triplets.size();
    }

    @Override public boolean isEmpty() {
        return triplets.isEmpty();
    }

    @Override public boolean contains( Object o ) {
        return triplets.contains( o );
    }

    @Override public Iterator<DelegatedUUIDList> iterator() {
        return triplets.iterator();
    }

    @Override public Object[] toArray() {
        return triplets.toArray();
    }

    @Override public <T> T[] toArray( T[] a ) {
        return triplets.toArray( a );
    }

    @Override public boolean add( DelegatedUUIDList uuids ) {
        Preconditions.checkArgument( uuids.size() == 3, "A triplet must contain exactly three elements." );
        return triplets.add( uuids );
    }

    @Override public boolean remove( Object o ) {
        return triplets.remove( o );
    }

    @Override public boolean containsAll( Collection<?> c ) {
        return triplets.removeAll( c );
    }

    @Override public boolean addAll( Collection<? extends DelegatedUUIDList> c ) {
        return triplets.addAll( c );
    }

    @Override public boolean retainAll( Collection<?> c ) {
        return triplets.retainAll( c );
    }

    @Override public boolean removeAll( Collection<?> c ) {
        return triplets.removeAll( c );
    }

    @Override public void clear() {
        triplets.clear();
    }

    @Override public boolean equals( Object o ) {
        return triplets.equals( o );
    }

    @Override public int hashCode() {
        return triplets.hashCode();
    }
}
