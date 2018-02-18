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

package com.openlattice.datastore.util;

import java.util.Iterator;

public class JacksonCassandraIterableWrapper<T> implements Iterable<T> {
    private final Iterable<T> iterable;

    public JacksonCassandraIterableWrapper( Iterable<T> iterable ) {
        this.iterable = iterable;
    }

    @Override
    public Iterator<T> iterator() {
        return new JacksonCassandraIteratorWrapper<T>( iterable.iterator() );
    }

    public static <T> Iterable<T> wrap( Iterable<T> i ) {
        return new JacksonCassandraIterableWrapper<T>( i );
    }

    public static class JacksonCassandraIteratorWrapper<T> implements Iterator<T> {
        private final Iterator<T> iterator;

        public JacksonCassandraIteratorWrapper( Iterator<T> iterator ) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public T next() {
            return iterator.next();
        }

    }

}
