

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

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.map.IMap;

public final class Util {
    private static final Logger logger = LoggerFactory.getLogger( Util.class );

    private Util() {}


    public static <K, V> V getSafely( IMap<K, V> m, K key ) {
        return m.get( key );
    }

    public static <K, V> Map<K,V> getSafely( IMap<K, V> m, Set<K> keys ) {
        return m.getAll( keys );
    }

    public static <K, V> V getSafely( Map<K, V> m, K key ) {
        return m.get( key );
    }

    public static <K, V> void deleteSafely( IMap<K, V> m, K key ) {
        m.delete( key );
    }

    public static <K, V> Function<K, V> getSafeMapper( IMap<K, V> m ) {
        return m::get;
    }

    public static <K, V> Consumer<? super K> safeDeleter( IMap<K, V> m ) {
        return m::delete;
    }

    public static <K, V> V removeSafely( IMap<K, V> fqns, K organizationId ) {
        return fqns.remove( organizationId );
    }
    
    public static String fqnToString( FullQualifiedName fqn ){
        return fqn.getFullQualifiedNameAsString();
    }
    
    public static FullQualifiedName stringToFqn( String string ){
        return new FullQualifiedName( string );
    }

    public static Set<String> fqnToString( Set<FullQualifiedName> fqns ){
        return fqns.stream().map( Util::fqnToString ).collect( Collectors.toSet() );
    }

    public static <T> T returnAndLog( T obj, String msg, Object... args ) {
        logger.info( msg, args );
        return obj;
    }
}
