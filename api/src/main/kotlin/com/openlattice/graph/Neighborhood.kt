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

import com.openlattice.data.Property
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

data class Neighborhood (
        val ids: Set<UUID>,
        // entity set id -> entity key id -> property, only issue is that this doesn't reflect the fact that property values are unique
        // issue that property values may not all be valid keys in json map (in particular location data)
        val entities: Map<UUID, Map<UUID, Map<UUID,Set<Any>>>>,
        //self -> association entity set id -> neighbor entity setid -> Pair(association ek id, neighbor ek id)
        val associations: Map<UUID, Map<UUID, Map<UUID, NeighborIds>>>
)