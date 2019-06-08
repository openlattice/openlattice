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

package com.openlattice.graph

import java.util.*

/**
 * @param ids The entity key ids for which to build the neighborhood
 * @param srcSelections The list of neighborhood selections where the ids are the destination. (src + filter > edge + filter > ids)
 * @param dstSelections The list of neighborhood selections where the ids are the source. (ids > edge + filter > dst + filter)
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class NeighborhoodQuery(
        val ids: Set<UUID>,
        val srcSelections: List<NeighborhoodSelection>,
        val dstSelections: List<NeighborhoodSelection>
)

