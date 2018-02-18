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

import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.SetMultimap;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntityDelegate implements SetMultimap<UUID, Object> {

    private final SetMultimap<UUID, Object> m;

    public EntityDelegate( SetMultimap<UUID, Object> m ) {
        this.m = m;
    }

    @Override public Set<Object> get( @Nullable UUID key ) {
        return m.get( key );
    }

    @Override public Set<Object> removeAll( @Nullable Object key ) {
        return m.removeAll( key );
    }

    @Override public Set<Object> replaceValues( UUID key, Iterable<?> values ) {
        return m.replaceValues( key, values );
    }

    @Override public Set<Entry<UUID, Object>> entries() {
        return m.entries();
    }

    @Override public Map<UUID, Collection<Object>> asMap() {
        return m.asMap();
    }

    @Override public boolean equals( Object obj ) {
        return m.equals( obj );
    }

    @Override public int size() {
        return m.size();
    }

    @Override public boolean isEmpty() {
        return m.isEmpty();
    }

    @Override public boolean containsKey( @Nullable Object key ) {
        return m.containsKey( key );
    }

    @Override public boolean containsValue( @Nullable Object value ) {
        return m.containsValue( value );
    }

    @Override public boolean containsEntry( @Nullable Object key, @Nullable Object value ) {
        return m.containsEntry( key, value );
    }

    @Override public boolean put( @Nullable UUID key, @Nullable Object value ) {
        return m.put( key, value );
    }

    @Override public boolean remove( @Nullable Object key, @Nullable Object value ) {
        return m.remove( key, value );
    }

    @Override public boolean putAll( @Nullable UUID key, Iterable<?> values ) {
        return m.putAll( key, values );
    }

    @Override public boolean putAll( Multimap<? extends UUID, ?> multimap ) {
        return m.putAll( multimap );
    }

    @Override public void clear() {
        m.clear();
    }

    @Override public Set<UUID> keySet() {
        return m.keySet();
    }

    @Override public Multiset<UUID> keys() {
        return m.keys();
    }

    @Override public Collection<Object> values() {
        return m.values();
    }

    @Override public int hashCode() {
        return m.hashCode();
    }
}
