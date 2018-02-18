

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
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openlattice.edm.internal.DatastoreConstants;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.core.IMap;

public final class Util {
    private static final Logger logger = LoggerFactory.getLogger( Util.class );

    private Util() {}

    public static <T> T getFutureSafely( ListenableFuture<T> futurePropertyType ) {
        try {
            return futurePropertyType.get();
        } catch ( InterruptedException | ExecutionException e1 ) {
            logger.error( "Failed to load {} type",
                    futurePropertyType.getClass().getTypeParameters()[ 0 ].getTypeName() );
            return null;
        }
    }

    public static boolean wasLightweightTransactionApplied( ResultSet rs ) {
        return wasLightweightTransactionApplied( rs.one() );
    }
    
    public static boolean wasLightweightTransactionApplied( Row row ){
        if ( row == null ) {
            return true;
        } else {
            return row.getBool( DatastoreConstants.APPLIED_FIELD );
        }
    }

    public static long getCount( ResultSet rs ) {
        return rs.one().getLong( DatastoreConstants.COUNT_FIELD );
    }

    public static boolean isCountNonZero( ResultSet rs ) {
        return getCount( rs ) > 0;
    }

    public static String toUnhyphenatedString( UUID uuid ) {
        return Long.toString( uuid.getLeastSignificantBits() ) + "_" + Long.toString( uuid.getMostSignificantBits() );
    }

    public static <T> Function<Row, T> transformSafelyFactory( Function<Row, T> f ) {
        return ( Row row ) -> transformSafely( row, f );

    }

    public static <T> T transformSafely( Row row, Function<Row, T> f ) {
        if ( row == null ) {
            return null;
        }
        return f.apply( row );
    }

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

    public static <K, V> V removeSafely(
            IMap<K, V> fqns,
            K organizationId ) {
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
