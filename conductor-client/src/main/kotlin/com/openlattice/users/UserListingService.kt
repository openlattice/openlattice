/*
 * Copyright (C) 2020. OpenLattice, Inc.
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

package com.openlattice.users

import com.auth0.json.mgmt.users.User
import java.time.Instant

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface UserListingService {
    /**
     * Retrieves all users as a sequence.
     */
    fun getAllUsers() : Sequence<User>

    /**
     * Retrieves updated users where update was happening after [from] (exclusive) and at or before [to] (inclusive)
     * as a sequence.
     */
    fun getUpdatedUsers(from: Instant, to: Instant) : Sequence<User>
}