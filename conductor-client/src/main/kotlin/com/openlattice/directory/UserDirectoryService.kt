/*
 * Copyright (C) 2019. OpenLattice, Inc.
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

package com.openlattice.directory

import com.auth0.json.mgmt.users.User
import com.codahale.metrics.annotation.Timed
import com.openlattice.search.Auth0UserSearchFields

internal const val DEFAULT_PAGE_SIZE = 100
internal const val SEARCH_ENGINE_VERSION = "v3"

interface UserDirectoryService {

    @Timed
    fun getAllUsers(): Map<String, User>

    @Timed
    fun getUser(userId: String): User

    @Timed
    fun getUsers(userIds: Set<String>): Map<String, User>

    //TODO: Switch over to a Hazelcast map to relieve pressure from Auth0
    @Timed
    fun searchAllUsers(fields: Auth0UserSearchFields): Map<String, User>

    @Timed
    fun deleteUser(userId: String)
}




