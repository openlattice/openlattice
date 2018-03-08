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

package com.openlattice.authorization;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PostgresUserApiTests extends HzAuthzTest {
    private static final String USERNAME = "oltest1";
    private static final String CRED1    = "test1";
    private static final String CRED2    = "test2";
    private static PostgresUserApi api;

    static {
        api = testServer.getContext().getBean( PostgresUserApi.class );
    }

    @Test
    public void testCreateUser() {
        //Ensure user doesn't exists
        deleteUser();
        //Create the user
        Assert.assertTrue( "User must be created.", createUser() );
    }

    @Test
    public void testDeleteUser() {
        createUser();
        Assert.assertTrue( "User must be deleted.", deleteUser() );
    }

    @Test
    public void testAlterUser() {
        createUser();
        Assert.assertTrue( "User must be altered", alterUser() );
    }

    @Test
    public void testCreateDuplicateUser() {
        deleteUser();
        Assert.assertTrue( "User must be created", createUser() );
        Assert.assertFalse( "User exists and should not be recreated.", createUser() );
    }

    @Test
    public void testDeleteUserTwice() {
        createUser();
        Assert.assertTrue( "User must be deleted.", deleteUser() );
        Assert.assertFalse( "User doesn't exists and should not be deletable.", deleteUser() );
    }

    @Test
    public void testExists() {
        deleteUser();
        Assert.assertFalse( "User must not exist", userExists() );
        Assert.assertTrue( "User must be created", createUser() );
        Assert.assertTrue( "User must exist", userExists() );
    }

    private static boolean userExists() {
        return api.exists( USERNAME );
    }

    private static boolean createUser() {
        return api.createUser( USERNAME, CRED1 );
    }

    private static boolean deleteUser() {
        return api.deleteUser( USERNAME );
    }

    private static boolean alterUser() {
        return api.setUserCredential( USERNAME, CRED2 );
    }
}
