

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

package com.openlattice.hazelcast.serializers;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.IoPerformingConsumer;
import com.kryptnostic.rhizome.hazelcast.serializers.IoPerformingFunction;
import com.openlattice.authorization.Permission;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class StreamSerializerUtils {
    private StreamSerializerUtils() {};

    private static final Permission[] permissions = Permission.values();

    public static <T> void serializeFromList( ObjectDataOutput out, List<T> elements, IoPerformingConsumer<T> c )
            throws IOException {
        out.writeInt( elements.size() );
        for ( T elem : elements ) {
            c.accept( elem );
        }
    }

    public static <T> List<T> deserializeToList(
            ObjectDataInput in,
            List<T> list,
            int size,
            IoPerformingFunction<ObjectDataInput, T> f )
            throws IOException {
        for ( int i = 0; i < size; ++i ) {
            T elem = f.apply( in );
            if ( elem != null ) {
                list.add( elem );
            }
        }
        return list;
    }

    public static <T> List<T> deserializeToList( ObjectDataInput in, IoPerformingFunction<ObjectDataInput, T> f )
            throws IOException {
        int size = in.readInt();
        return deserializeToList( in, Lists.newArrayListWithExpectedSize( size ), size, f );
    }

    public static <K, V> void serializeFromMap(
            ObjectDataOutput out,
            Map<K, V> elements,
            IoPerformingConsumer<K> cK,
            IoPerformingConsumer<V> cV )
            throws IOException {
        out.writeInt( elements.size() );
        for ( Map.Entry<K, V> elem : elements.entrySet() ) {
            cK.accept( elem.getKey() );
            cV.accept( elem.getValue() );
        }
    }

    public static <K, V> Map<K, V> deserializeToMap(
            ObjectDataInput in,
            Map<K, V> map,
            int size,
            IoPerformingFunction<ObjectDataInput, K> fK,
            IoPerformingFunction<ObjectDataInput, V> fV )
            throws IOException {
        for ( int i = 0; i < size; ++i ) {
            K key = fK.apply( in );
            V value = fV.apply( in );
            if ( key != null ) {
                map.put( key, value );
            }
        }
        return map;
    }

    public static <K, V> Map<K, V> deserializeToMap(
            ObjectDataInput in,
            IoPerformingFunction<ObjectDataInput, K> fK,
            IoPerformingFunction<ObjectDataInput, V> fV )
            throws IOException {
        int size = in.readInt();
        return deserializeToMap( in, Maps.newHashMapWithExpectedSize( size ), size, fK, fV );
    }

    public static void serializeFromPermissionEnumSet( ObjectDataOutput out, EnumSet<Permission> object ) throws IOException {
        out.writeInt( object.size() );
        for ( Permission permission : object ) {
            out.writeInt( permission.ordinal() );
        }
    }

    public static EnumSet<Permission> deserializeToPermissionEnumSet( ObjectDataInput in ) throws IOException {
        int size = in.readInt();
        EnumSet<Permission> set = EnumSet.noneOf( Permission.class );
        
        for ( int i = 0; i < size; ++i ) {
            set.add( permissions[ in.readInt() ] );
        }
        return set;
    }

}
