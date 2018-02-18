

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

package com.openlattice.hazelcast;

import com.hazelcast.core.IMap;
import com.openlattice.datastore.util.Util;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

public class HazelcastUtils {
    public static <T, V> V typedGet( IMap<T, V> m, T key ) {
        return m.get( key );
    }

    public static <K, V> K insertIntoUnusedKey( IMap<K, V> m, V value, Supplier<K> keyFactory ) {
        K key = keyFactory.get();
        while ( m.putIfAbsent( key, value ) != null ) {
            key = keyFactory.get();
        }
        return key;
    }

    public static <K, V> Function<K, V> getter( IMap<K, V> m ) {
        return ( K k ) -> Util.getSafely( m, k );
    }

    public static DelegatedUUIDList hzList( List<UUID> ids ) {
        return DelegatedUUIDList.wrap( ids );
    }
}
