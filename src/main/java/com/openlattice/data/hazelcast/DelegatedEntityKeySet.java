/*
 * Copyright (C) 2017. OpenLattice, Inc
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

package com.openlattice.data.hazelcast;

import com.openlattice.data.EntityKey;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class DelegatedEntityKeySet implements Set<EntityKey> {
    private final Set<EntityKey> keys;

    public DelegatedEntityKeySet( Set<EntityKey> keys ) {
        this.keys = keys;
    }

    public static DelegatedEntityKeySet wrap( Set<EntityKey> keys ) {
        return new DelegatedEntityKeySet( keys );
    }

    public Set<EntityKey> unwrap() {
        return keys;
    }

    public void forEach( Consumer<? super EntityKey> action ) {
        keys.forEach( action );
    }

    public int size() {
        return keys.size();
    }

    public boolean isEmpty() {
        return keys.isEmpty();
    }

    public boolean contains( Object o ) {
        return keys.contains( o );
    }

    public Iterator<EntityKey> iterator() {
        return keys.iterator();
    }

    public Object[] toArray() {
        return keys.toArray();
    }

    public <T> T[] toArray( T[] a ) {
        return keys.toArray( a );
    }

    public boolean add( EntityKey e ) {
        return keys.add( e );
    }

    public boolean remove( Object o ) {
        return keys.remove( o );
    }

    public boolean containsAll( Collection<?> c ) {
        return keys.containsAll( c );
    }

    public boolean addAll( Collection<? extends EntityKey> c ) {
        return keys.addAll( c );
    }

    public boolean retainAll( Collection<?> c ) {
        return keys.retainAll( c );
    }

    public boolean removeAll( Collection<?> c ) {
        return keys.removeAll( c );
    }

    public void clear() {
        keys.clear();
    }

    public boolean equals( Object o ) {
        return keys.equals( o );
    }

    public int hashCode() {
        return keys.hashCode();
    }

    public Spliterator<EntityKey> spliterator() {
        return keys.spliterator();
    }

    public boolean removeIf( Predicate<? super EntityKey> filter ) {
        return keys.removeIf( filter );
    }

    public Stream<EntityKey> stream() {
        return keys.stream();
    }

    public Stream<EntityKey> parallelStream() {
        return keys.parallelStream();
    }
}