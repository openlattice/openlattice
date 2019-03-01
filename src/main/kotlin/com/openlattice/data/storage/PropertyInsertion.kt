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

package com.openlattice.data.storage

import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
internal data class PropertyInsertion(
        val propertyTypeId: UUID,
        val entityKeyId: UUID,
        val propertyHash: ByteArray,
        val insertValue: Any
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is PropertyInsertion) return false

        if (propertyTypeId != other.propertyTypeId) return false
        if (entityKeyId != other.entityKeyId) return false
        if (!propertyHash.contentEquals(other.propertyHash)) return false
        if (insertValue != other.insertValue) return false

        return true
    }

    override fun hashCode(): Int {
        var result = propertyTypeId.hashCode()
        result = 31 * result + entityKeyId.hashCode()
        result = 31 * result + propertyHash.contentHashCode()
        result = 31 * result + insertValue.hashCode()
        return result
    }
}
