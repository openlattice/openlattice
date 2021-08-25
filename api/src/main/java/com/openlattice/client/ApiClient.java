/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

package com.openlattice.client;

import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.openlattice.apps.AppApi;
import com.openlattice.authorization.AuthorizationsApi;
import com.openlattice.authorization.PermissionsApi;
import com.openlattice.client.RetrofitFactory.Environment;
import com.openlattice.client.serialization.SerializableSupplier;
import com.openlattice.collections.CollectionsApi;
import com.openlattice.data.DataApi;
import com.openlattice.data.DataIntegrationApi;
import com.openlattice.data.S3Api;
import com.openlattice.directory.PrincipalApi;
import com.openlattice.edm.EdmApi;
import com.openlattice.entitysets.EntitySetsApi;
import com.openlattice.graph.GraphApi;
import com.openlattice.organization.OrganizationsApi;
import com.openlattice.search.SearchApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class ApiClient implements ApiFactoryFactory {

    private static final long serialVersionUID = -5757911484718872922L;

    private static final Logger                         logger      = LoggerFactory
            .getLogger( ApiClient.class );
    private final        ApiFactoryFactory              retrofitSupplier;
    private transient    Supplier<ApiFactory>           restAdapter = null;
    private transient    LoadingCache<Class<?>, Object> apiCache    = null;

    public ApiClient( Environment environment, SerializableSupplier<String> jwtToken ) {
        this( () -> {
            final Retrofit retrofit = RetrofitFactory.newClient( environment, jwtToken );
            return (ApiFactory) retrofit::create;
        } );
    }

    public ApiClient( SerializableSupplier<String> jwtToken ) {
        this( () -> {
            final Retrofit retrofit = RetrofitFactory.newClient( jwtToken );
            return (ApiFactory) retrofit::create;
        } );
    }

    public ApiClient( ApiFactoryFactory retrofitSupplier ) {
        this.retrofitSupplier = retrofitSupplier;
        logger.info( "API client ready!" );
    }

    public AppApi getAppApi() throws ExecutionException {
        return get().create( AppApi.class );
    }

    public AuthorizationsApi getAuthorizationsApi() throws ExecutionException {
        return get().create( AuthorizationsApi.class );
    }

    public DataIntegrationApi getDataIntegrationApi() {
        return get().create( DataIntegrationApi.class );
    }

    public DataApi getDataApi() throws ExecutionException {
        return get().create( DataApi.class );
    }

    public PermissionsApi getPermissionsApi() throws ExecutionException {
        return get().create( PermissionsApi.class );
    }

    public PrincipalApi getPrincipalApi() throws ExecutionException {
        return get().create( PrincipalApi.class );
    }

    public EdmApi getEdmApi() throws ExecutionException {
        return get().create( EdmApi.class );
    }

    public EntitySetsApi getEntitySetsApi() throws ExecutionException {
        return get().create( EntitySetsApi.class );
    }

    public OrganizationsApi getOrganizationsApi() throws ExecutionException {
        return get().create( OrganizationsApi.class );
    }

    public CollectionsApi getCollectionsApi() throws ExecutionException {
        return get().create( CollectionsApi.class );
    }

    public GraphApi getGraphApi() throws ExecutionException {
        return get().create( GraphApi.class );
    }

    public SearchApi getSearchApi() {
        return get().create( SearchApi.class );
    }

    public S3Api getS3Api() {
        return get().create( S3Api.class );
    }

    public ApiFactory get() {
        if ( apiCache == null ) {
            apiCache = CacheBuilder.newBuilder()
                    .maximumSize( 100 )
                    .initialCapacity( 10 )
                    .build( new CacheLoader<>() {
                        private final Supplier<ApiFactory> apiFactory = Suppliers.memoize( retrofitSupplier::get )::get;

                        @Override
                        public Object load( Class<?> key ) throws Exception {
                            return apiFactory.get().create( key );
                        }
                    } );
        }

        return apiCache::getUnchecked;
    }

    public static SerializableSupplier<ApiClient> getSerializableSupplier( SerializableSupplier<String> jwtToken ) {
        return () -> new ApiClient( jwtToken );
    }

    public static SerializableSupplier<ApiClient> getSerializableSupplier(
            Environment environment,
            SerializableSupplier<String> jwtToken ) {
        return () -> new ApiClient( environment, jwtToken );
    }
}
