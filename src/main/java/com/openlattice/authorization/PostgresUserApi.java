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

import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public interface PostgresUserApi {

    @SqlQuery("select count(*)=1 from pg_roles where rolname=:id")
    boolean exists( @Bind("id") String userId );
    /**
     *
     * @param userId
     * @param credential
     * @return
     */
    @SqlQuery("select create_ol_user(:id,:cred)")
    boolean createUser( @Bind("id") String userId, @Bind("cred") String credential );

    /**
     *
     * @param userId
     * @param credential
     * @return
     */
    @SqlQuery("select alter_ol_user(:id,:cred)")
    boolean setUserCredential( @Bind("id") String userId, @Bind("cred") String credential );

    /**
     * Deletes the specified user from Postgres internal user accounts.
     * @param userId The user to delete.
     * @return True if the user was deleted, false otherwise.
     */
    @SqlQuery("select delete_ol_user(:id)")
    boolean deleteUser( @Bind("id") String userId );
}
