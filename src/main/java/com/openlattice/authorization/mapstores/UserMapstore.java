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

package com.openlattice.authorization.mapstores;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.kryptnostic.rhizome.mapstores.TestableSelfRegisteringMapStore;
import com.openlattice.auth0.Auth0TokenProvider;
import com.openlattice.authentication.Auth0Configuration;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.datastore.services.Auth0ManagementApi;
import com.openlattice.directory.pojo.Auth0UserBasic;
import com.openlattice.hazelcast.HazelcastMap;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class UserMapstore implements TestableSelfRegisteringMapStore<String, Auth0UserBasic> {
    public static final String LOAD_TIME_INDEX = "loadTime";
    private static final Logger logger            = LoggerFactory.getLogger( UserMapstore.class );
    private static final int    DEFAULT_PAGE_SIZE = 100;
    private static final int    TTL_SECONDS       = 360;
    private final Retrofit           retrofit;
    private final Auth0ManagementApi auth0ManagementApi;

    public UserMapstore( Auth0TokenProvider auth0TokenProvider ) {
        retrofit = RetrofitFactory.newClient( auth0TokenProvider.getManagementApiUrl(), auth0TokenProvider::getToken );
        auth0ManagementApi = retrofit.create( Auth0ManagementApi.class );
    }

    @Override public String getMapName() {
        return HazelcastMap.USERS.name();
    }

    @Override public String getTable() {
        //There is no table for auth0
        return null;
    }

    @Override public String generateTestKey() {
        return null;
    }

    @Override public Auth0UserBasic generateTestValue() {
        return null;
    }

    @Override public MapConfig getMapConfig() {
        return new MapConfig( getMapName() )
                .setTimeToLiveSeconds( TTL_SECONDS )
                .addMapIndexConfig( new MapIndexConfig( LOAD_TIME_INDEX, true ) )
                .setMapStoreConfig( getMapStoreConfig() );
    }

    @Override public MapStoreConfig getMapStoreConfig() {
        return new MapStoreConfig()
                .setImplementation( this )
                .setEnabled( false );

    }

    @Override public void store( String key, Auth0UserBasic value ) {
        throw new NotImplementedException( "Auth0 persistence not implemented." );
    }

    @Override public void storeAll( Map<String, Auth0UserBasic> map ) {
        throw new NotImplementedException( "Auth0 persistence not implemented." );
    }

    @Override public void delete( String key ) {
        throw new NotImplementedException( "Auth0 persistence not implemented." );
    }

    @Override public void deleteAll( Collection<String> keys ) {
        throw new NotImplementedException( "Auth0 persistence not implemented." );
    }

    @Override public Auth0UserBasic load( String userId ) {
        return auth0ManagementApi.getUser( userId );
    }

    @Override public Map<String, Auth0UserBasic> loadAll( Collection<String> keys ) {
        return keys.stream()
                .collect( Collectors.toMap( Function.identity(), this::load ) );
    }

    @Override public Iterable<String> loadAllKeys() {
        return null;
    }
}
