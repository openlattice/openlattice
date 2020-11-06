

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

package com.openlattice.organizations;

import com.openlattice.authorization.Principal;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class PrincipalSet implements Set<Principal> {
    private Set<Principal> principals;

    public PrincipalSet( Set<Principal> principals ) {
        this.principals = principals;
    }

    public Set<Principal> unwrap() {
        return principals;
    }

    public void forEach( Consumer<? super Principal> action ) {
        principals.forEach( action );
    }

    public int size() {
        return principals.size();
    }

    public boolean isEmpty() {
        return principals.isEmpty();
    }

    public boolean contains( Object o ) {
        return principals.contains( o );
    }

    public Iterator<Principal> iterator() {
        return principals.iterator();
    }

    public Object[] toArray() {
        return principals.toArray();
    }

    public <T> T[] toArray( T[] a ) {
        return principals.toArray( a );
    }

    public boolean add( Principal e ) {
        return principals.add( e );
    }

    public boolean remove( Object o ) {
        return principals.remove( o );
    }

    public boolean containsAll( Collection<?> c ) {
        return principals.containsAll( c );
    }

    public boolean addAll( Collection<? extends Principal> c ) {
        return principals.addAll( c );
    }

    public boolean retainAll( Collection<?> c ) {
        return principals.retainAll( c );
    }

    public boolean removeAll( Collection<?> c ) {
        return principals.removeAll( c );
    }

    public void clear() {
        principals.clear();
    }

    public boolean equals( Object o ) {
        return principals.equals( o );
    }

    public int hashCode() {
        return principals.hashCode();
    }

    public Spliterator<Principal> spliterator() {
        return principals.spliterator();
    }

    public boolean removeIf( Predicate<? super Principal> filter ) {
        return principals.removeIf( filter );
    }

    public Stream<Principal> stream() {
        return principals.stream();
    }

    public Stream<Principal> parallelStream() {
        return principals.parallelStream();
    }

    public static PrincipalSet wrap( Set<Principal> principals ) {
        return new PrincipalSet( principals );
    }

    @Override
    public String toString() {
        return "PrincipalSet{" + principals.toString() + "}";
    }

}
