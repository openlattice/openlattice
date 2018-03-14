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

import static com.google.common.base.Preconditions.checkState;

import com.auth0.json.auth.TokenHolder;
import com.google.common.util.concurrent.RateLimiter;
import com.openlattice.authentication.AuthenticationTest;
import com.openlattice.authentication.AuthenticationTestRequestOptions;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.Principals;
import com.openlattice.authorization.SecurablePrincipal;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.directory.PrincipalApi;
import com.openlattice.directory.pojo.Auth0UserBasic;
import com.openlattice.edm.EdmApi;
import java.util.Collection;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.security.core.Authentication;
import retrofit2.Retrofit;

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

    protected static final Principal admin;
    protected static final Principal user1;
    protected static final Principal user2;
    protected static final Principal user3;
    protected static final Retrofit  retrofit;
    protected static final Retrofit  retrofit1;
    protected static final Retrofit  retrofit2;
    protected static final Retrofit  retrofit3;

    static {
        RateLimiter limiter = RateLimiter.create( .5 );

        limiter.acquire();
        Authentication jwtAdmin = AuthenticationTest.authenticate();
        Authentication jwtUser1 = AuthenticationTest.getAuthentication( authOptions1 );
        limiter.acquire();
        Authentication jwtUser2 = AuthenticationTest.getAuthentication( authOptions2 );
        Authentication jwtUser3 = AuthenticationTest.getAuthentication( authOptions3 );

        limiter.acquire();
        TokenHolder thAdmin = AuthenticationTest.tokenHolder();
        TokenHolder thUser1 = AuthenticationTest.tokenHolder( authOptions1 );
        limiter.acquire();
        TokenHolder thUser2 = AuthenticationTest.tokenHolder( authOptions2 );
        TokenHolder thUser3 = AuthenticationTest.tokenHolder( authOptions3 );

        String tokenAdmin = (String) jwtAdmin.getCredentials();
        String tokenUser1 = (String) jwtUser1.getCredentials();
        String tokenUser2 = (String) jwtUser2.getCredentials();
        String tokenUser3 = (String) jwtUser3.getCredentials();

        retrofit = RetrofitFactory.newClient( RetrofitFactory.Environment.TESTING, () -> tokenAdmin );
        retrofit1 = RetrofitFactory.newClient( RetrofitFactory.Environment.TESTING, () -> tokenUser1 );
        retrofit2 = RetrofitFactory.newClient( RetrofitFactory.Environment.TESTING, () -> tokenUser2 );
        retrofit3 = RetrofitFactory.newClient( RetrofitFactory.Environment.TESTING, () -> tokenUser3 );

        String idAdmin = (String) jwtAdmin.getPrincipal();
        String idUser1 = (String) jwtUser1.getPrincipal();
        String idUser2 = (String) jwtUser2.getPrincipal();
        String idUser3 = (String) jwtUser3.getPrincipal();

        Collection<SecurablePrincipal> rolesAdmin = retrofit.create( PrincipalApi.class )
                .activateUser( thAdmin.getAccessToken() );
        Collection<SecurablePrincipal> rolesUser1 = retrofit1.create( PrincipalApi.class )
                .activateUser( thUser1.getAccessToken() );
        Collection<SecurablePrincipal> rolesUser2 = retrofit2.create( PrincipalApi.class )
                .activateUser( thUser2.getAccessToken() );
        Collection<SecurablePrincipal> rolesUser3 = retrofit3.create( PrincipalApi.class )
                .activateUser( thUser3.getAccessToken() );

        ensurePrincipalHasPrincipalsWithName( rolesAdmin, "admin" );

        ensurePrincipalHasPrincipalsWithName( rolesAdmin, "AuthenticatedUser" );
        ensurePrincipalHasPrincipalsWithName( rolesUser1, "AuthenticatedUser" );
        ensurePrincipalHasPrincipalsWithName( rolesUser2, "AuthenticatedUser" );
        ensurePrincipalHasPrincipalsWithName( rolesUser3, "AuthenticatedUser" );

        admin = toPrincipal( idAdmin );
        user1 = toPrincipal( idUser1 );
        user2 = toPrincipal( idUser2 );
        user3 = toPrincipal( idUser3 );

        TestEdmConfigurer.setupDatamodel( retrofit.create( EdmApi.class ) );
    }

    @Test
    public void testLoadUser() {
        PrincipalApi pApi = retrofit.create( PrincipalApi.class );
        Auth0UserBasic adminUser = pApi.getUser( admin.getId() );
        Assert.assertTrue( adminUser.getRoles().contains( "admin" ) );
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
