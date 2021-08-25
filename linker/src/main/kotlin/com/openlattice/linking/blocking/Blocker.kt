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

package com.openlattice.linking.blocking

import com.openlattice.data.EntityDataKey
import com.openlattice.linking.Block

/**
 *
 * Interface for components that can perform the blocking step in the linking process.
 *
 */
interface Blocker {
    /**
     * Retrieves the top 50 matches per entity set. This can be a large number of search results 1000 * # entity sets.
     *
     * @param entityDataKey The entity data key id of the entity upon which to perform blocking.
     * @return A block of potentially matching objects as a mapping from entity data keys to entity properties
     *
     */
    fun block(
            entityDataKey: EntityDataKey,
            top: Int = 50
    ): Block

}