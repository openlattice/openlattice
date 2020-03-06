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
 *
 */

package com.openlattice.rehearsal;

import com.auth0.json.mgmt.users.User;
import com.geekbeast.util.RunOnce;
import com.google.common.util.concurrent.RateLimiter;
import com.openlattice.authentication.AuthenticationTest;
import com.openlattice.authentication.AuthenticationTestRequestOptions;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.Principals;
import com.openlattice.authorization.SecurablePrincipal;
import com.openlattice.authorization.SystemRole;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;
import com.openlattice.directory.PrincipalApi;
import kotlin.Unit;
import okhttp3.OkHttpClient;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import retrofit2.Retrofit;

import java.util.Collection;

import static com.google.common.base.Preconditions.checkState;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class SetupEnvironment {
    protected static final AuthenticationTestRequestOptions authOptions = new AuthenticationTestRequestOptions()
            .setUsernameOrEmail( "tests@openlattice.com" )
            .setPassword( "openlattice" );
    protected static final AuthenticationTestRequestOptions authOptions1 = new AuthenticationTestRequestOptions()
            .setUsernameOrEmail( "tests1@openlattice.com" )
            .setPassword( "abracadabra" );
    protected static final AuthenticationTestRequestOptions authOptions2 = new AuthenticationTestRequestOptions()
            .setUsernameOrEmail( "tests2@openlattice.com" )
            .setPassword( "abracadabra" );
    protected static final AuthenticationTestRequestOptions authOptions3 = new AuthenticationTestRequestOptions()
            .setUsernameOrEmail( "tests3@openlattice.com" )
            .setPassword( "abracadabra" );

    protected static Principal admin;
    protected static Principal user1;
    protected static Principal user2;
    protected static Principal user3;
    protected static Retrofit retrofit;
    protected static Retrofit retrofitLinker;
    protected static Retrofit retrofit1;
    protected static Retrofit retrofit2;
    protected static Retrofit retrofit3;
    protected static Retrofit retrofitProd;
    protected static OkHttpClient httpClient;
    protected static OkHttpClient httpClient1;
    protected static OkHttpClient httpClient2;

    protected static final Logger logger = LoggerFactory.getLogger( SetupEnvironment.class );

    static RunOnce runOnce = new RunOnce(SetupEnvironment::initialize);

    @BeforeClass
    public static void setupEnvironment() {
        runOnce.get();
    }

    private static Unit initialize() {
        RateLimiter limiter = RateLimiter.create( .5 );

        limiter.acquire();
        Authentication jwtAdmin = AuthenticationTest.authenticate();
        Authentication jwtUser1 = AuthenticationTest.getAuthentication( authOptions1 );
        limiter.acquire();
        Authentication jwtUser2 = AuthenticationTest.getAuthentication( authOptions2 );
        Authentication jwtUser3 = AuthenticationTest.getAuthentication( authOptions3 );

        String tokenAdmin = ( String ) jwtAdmin.getCredentials();
        String tokenUser1 = ( String ) jwtUser1.getCredentials();
        String tokenUser2 = ( String ) jwtUser2.getCredentials();
        String tokenUser3 = ( String ) jwtUser3.getCredentials();

        RetrofitFactory.configureObjectMapper( FullQualifiedNameJacksonSerializer::registerWithMapper );

        retrofit = RetrofitFactory.newClient( RetrofitFactory.Environment.TESTING, () -> tokenAdmin, new ThrowingCallAdapterFactory() );
        retrofitLinker = RetrofitFactory.newClient( RetrofitFactory.Environment.TESTING_LINKER, () -> tokenAdmin, new ThrowingCallAdapterFactory() );
        retrofit1 = RetrofitFactory.newClient( RetrofitFactory.Environment.TESTING, () -> tokenUser1, new ThrowingCallAdapterFactory() );
        retrofit2 = RetrofitFactory.newClient( RetrofitFactory.Environment.TESTING, () -> tokenUser2 );
        retrofit3 = RetrofitFactory.newClient( RetrofitFactory.Environment.TESTING, () -> tokenUser3 );
        retrofitProd = RetrofitFactory.newClient( RetrofitFactory.Environment.PRODUCTION );

        httpClient = RetrofitFactory.okHttpClientWithOpenLatticeAuth( () -> tokenAdmin ).build();
        httpClient1 = RetrofitFactory.okHttpClientWithOpenLatticeAuth( () -> tokenUser1 ).build();
        httpClient2 = RetrofitFactory.okHttpClientWithOpenLatticeAuth( () -> tokenUser2 ).build();

        String idAdmin = ( String ) jwtAdmin.getPrincipal();
        String idUser1 = ( String ) jwtUser1.getPrincipal();
        String idUser2 = ( String ) jwtUser2.getPrincipal();
        String idUser3 = ( String ) jwtUser3.getPrincipal();

        retrofit.create( PrincipalApi.class ).syncCallingUser();
        retrofit1.create( PrincipalApi.class ).syncCallingUser();
        retrofit2.create( PrincipalApi.class ).syncCallingUser();
        retrofit3.create( PrincipalApi.class ).syncCallingUser();

        final var rolesAdmin = retrofit.create(PrincipalApi.class).getCurrentRoles();
        final var rolesUser1 = retrofit1.create(PrincipalApi.class).getCurrentRoles();
        final var rolesUser2 = retrofit2.create(PrincipalApi.class).getCurrentRoles();
        final var rolesUser3 = retrofit3.create(PrincipalApi.class).getCurrentRoles();

        ensurePrincipalHasPrincipalsWithName( rolesAdmin, SystemRole.ADMIN.getName() );

        ensurePrincipalHasPrincipalsWithName( rolesAdmin, SystemRole.AUTHENTICATED_USER.getName() );
        ensurePrincipalHasPrincipalsWithName( rolesUser1, SystemRole.AUTHENTICATED_USER.getName() );
        ensurePrincipalHasPrincipalsWithName( rolesUser2, SystemRole.AUTHENTICATED_USER.getName() );
        ensurePrincipalHasPrincipalsWithName( rolesUser3, SystemRole.AUTHENTICATED_USER.getName() );

        admin = toPrincipal( idAdmin );
        user1 = toPrincipal( idUser1 );
        user2 = toPrincipal( idUser2 );
        user3 = toPrincipal( idUser3 );

        return Unit.INSTANCE;
    }

    public static User getUserInfo( Principal principal ) {
        PrincipalApi pApi = getApiAdmin( PrincipalApi.class );
        return pApi.getUser( principal.getId() );
    }

    public static void ensurePrincipalHasPrincipalsWithName(
            Collection<SecurablePrincipal> principals,
            String principalId ) {
        checkState( principals.stream().anyMatch( sp -> sp.getPrincipal().getId().equals( principalId ) ) );
    }

    protected static <T> T getApiAdmin( Class<T> clazz ) {
        return retrofit.create( clazz );
    }

    protected static <T> T getApiUser1( Class<T> clazz ) {
        return retrofit1.create( clazz );
    }

    protected static <T> T getApiUser2( Class<T> clazz ) {
        return retrofit2.create( clazz );
    }

    protected static <T> T getApiUser3( Class<T> clazz ) {
        return retrofit3.create( clazz );
    }

    private static Principal toPrincipal( String principalId ) {
        return Principals.getUserPrincipal( principalId );
    }
}
