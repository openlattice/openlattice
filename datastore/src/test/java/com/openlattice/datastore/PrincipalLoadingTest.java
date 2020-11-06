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

package com.openlattice.datastore;

import com.openlattice.client.RetrofitFactory;
import com.openlattice.directory.pojo.Auth0UserBasic;
import com.openlattice.datastore.services.Auth0ManagementApi;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;
import java.util.Set;

/**
 * This test requires credentials that will be wired in via bamboo. Don't have time right now to set it up.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Ignore
public class PrincipalLoadingTest {
    private static final Logger             logger             = LoggerFactory.getLogger( PrincipalLoadingTest.class );
    private static final String             token              = "";
    private static final Retrofit           retrofit           = RetrofitFactory.newClient( "https://openlattice.auth0.com/api/v2/", () -> token );
    private static final Auth0ManagementApi auth0ManagementApi = retrofit.create( Auth0ManagementApi.class );

    @Test
    public void testAuthRequest() {
        Set<Auth0UserBasic> users = auth0ManagementApi.getAllUsers(0,100);
        Assert.assertTrue( !users.isEmpty() );
    }

    @Test
    public void testGetUser() {
        Auth0UserBasic user = auth0ManagementApi.getUser("auth0|57e4b2d8d9d1d194778fd5b6" );
        logger.info( "This is the user that was retrieved: {}"  , user );
        Assert.assertNotNull( user );
    }


}
