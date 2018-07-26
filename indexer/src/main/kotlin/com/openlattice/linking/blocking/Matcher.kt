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

import com.google.common.collect.SetMultimap
import com.openlattice.data.EntityDataKey
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface Matcher {
    /**
     * Computes an approximation of the discrete metric of every pair of blocked entities.
     *
     * A computed match of 0.0 is 'close' and a match of 1.0 is 'far'.
     *
     * @param block An entity paired to a set of entities from across zero or more entity sets mapped by data key.
     * @return The computed match between all unique entities pairs in the block.
     */
    fun match(
            block: Map<EntityDataKey, SetMultimap<UUID, Any>>
    ): Map<EntityDataKey, Map<EntityDataKey, Double>>

}