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

package com.openlattice.graph.core

import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class NeighborSets(val srcEntitySetId: UUID, val edgeEntitySetId: UUID, val dstEntitySetId: UUID) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is NeighborSets) return false

        if (srcEntitySetId != other.srcEntitySetId) return false
        if (edgeEntitySetId != other.edgeEntitySetId) return false
        if (dstEntitySetId != other.dstEntitySetId) return false

        return true
    }

    override fun hashCode(): Int {
        var result = srcEntitySetId.hashCode()
        result = 31 * result + edgeEntitySetId.hashCode()
        result = 31 * result + dstEntitySetId.hashCode()
        return result
    }

    override fun toString(): String {
        return "NeighborSets(srcEntitySetId=$srcEntitySetId, edgeEntitySetId=$edgeEntitySetId, dstEntitySetId=$dstEntitySetId)"
    }

}