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

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openlattice.client.serialization.SerializableSupplier;
import com.openlattice.retrofit.RhizomeByteConverterFactory;
import com.openlattice.retrofit.RhizomeCallAdapterFactory;
import com.openlattice.retrofit.RhizomeJacksonConverterFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import okhttp3.OkHttpClient;
import retrofit2.CallAdapter;
import retrofit2.Retrofit;

public final class RetrofitFactory {
    private static final String BASE_URL                = "https://api.openlattice.com/";
    private static final String INTEGRATION_URL         = "https://integration.openlattice.com/";
    private static final String LOCAL_BASE_URL          = "http://localhost:8080/";
    private static final String STAGING_BASE_URL        = "https://api.staging.openlattice.com/";
    private static final String STAGING_INTEGRATION_URL = "https://integration.staging.openlattice.com/";
    private static final String TESTING_BASE_URL        = "http://localhost:8080/";
    private static final String TESTING_INDEXER_URL     = "http://localhost:8081/";
    private static final String TESTING_LINKER_URL      = "http://localhost:8082/";

    private static final ObjectMapper jsonMapper = ObjectMappers.getJsonMapper();

    private RetrofitFactory() {
    }

    public enum Environment {
        PROD_INTEGRATION( INTEGRATION_URL ),
        PRODUCTION( BASE_URL ),
        STAGING_INTEGRATION( STAGING_INTEGRATION_URL ),
        STAGING( STAGING_BASE_URL ),
        LOCAL( LOCAL_BASE_URL ),
        TESTING( TESTING_BASE_URL ),
        TESTING_INDEXER( TESTING_INDEXER_URL ),
        TESTING_LINKER( TESTING_LINKER_URL );

        private final String baseUrl;

        Environment( String baseUrl ) {
            this.baseUrl = baseUrl;
        }

        public String getBaseUrl() {
            return baseUrl;
        }
    }

    /**
     * Create a new client with no authentication
     */
    public static Retrofit newClient( Environment environment ) {
        return decorateWithOpenLatticeFactories(
                createBaseRhizomeRetrofitBuilder( environment, new OkHttpClient.Builder() )
        ).build();
    }

    public static Retrofit newClient( SerializableSupplier<String> jwtToken ) {
        return newClient( Environment.PRODUCTION, jwtToken );
    }

    public static Retrofit newClient(
            Environment environment,
            Supplier<String> jwtToken,
            CallAdapter.Factory callFactory ) {
        OkHttpClient.Builder httpBuilder = okHttpClientWithOpenLatticeAuth( jwtToken );
        return decorateWithFactories( createBaseRhizomeRetrofitBuilder( environment, httpBuilder ), callFactory )
                .build();
    }

    public static Retrofit newClient( Environment environment, Supplier<String> jwtToken ) {
        OkHttpClient.Builder httpBuilder = okHttpClientWithOpenLatticeAuth( jwtToken );
        return decorateWithOpenLatticeFactories( createBaseRhizomeRetrofitBuilder( environment, httpBuilder ) ).build();
    }

    public static Retrofit newClient( String baseUrl, Supplier<String> jwtToken ) {
        OkHttpClient.Builder httpBuilder = okHttpClientWithOpenLatticeAuth( jwtToken );
        return decorateWithOpenLatticeFactories( createBaseRhizomeRetrofitBuilder( baseUrl, httpBuilder.build() ) )
                .build();
    }

    public static Retrofit.Builder createBaseRhizomeRetrofitBuilder(
            Environment environment,
            OkHttpClient.Builder httpBuilder ) {
        return createBaseRhizomeRetrofitBuilder( environment.getBaseUrl(), httpBuilder.build() );
    }

    public static Retrofit.Builder createBaseRhizomeRetrofitBuilder( String baseUrl, OkHttpClient httpClient ) {
        return new Retrofit.Builder().baseUrl( baseUrl ).client( httpClient );
    }

    public static Retrofit.Builder decorateWithOpenLatticeFactories( Retrofit.Builder builder ) {
        return decorateWithFactories( builder, new RhizomeCallAdapterFactory() );
    }

    public static Retrofit.Builder decorateWithFactories( Retrofit.Builder builder, CallAdapter.Factory callFactory ) {
        return builder.addConverterFactory( new RhizomeByteConverterFactory() )
                .addConverterFactory( new RhizomeJacksonConverterFactory( jsonMapper ) )
                .addCallAdapterFactory( callFactory );
    }

    public static OkHttpClient.Builder okHttpClient() {
        return new OkHttpClient.Builder()
                .readTimeout( 0, TimeUnit.MILLISECONDS )
                .writeTimeout( 0, TimeUnit.MILLISECONDS )
                .connectTimeout( 0, TimeUnit.MILLISECONDS );
    }

    public static OkHttpClient.Builder okHttpClientWithOpenLatticeAuth( Supplier<String> jwtToken ) {
        return okHttpClient()
                .addInterceptor( chain -> chain.proceed(
                        chain.request().newBuilder()
                                .addHeader( "Authorization", "Bearer " + jwtToken.get() )
                                .build()
                        )
                );
    }

    public static void configureObjectMapper( Consumer<ObjectMapper> c ) {
        c.accept( jsonMapper );
    }
}
