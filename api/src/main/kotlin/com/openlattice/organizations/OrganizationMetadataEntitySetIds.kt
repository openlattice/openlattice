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

package com.openlattice.organizations

import com.fasterxml.jackson.annotation.JsonIgnore
import java.io.Serializable
import java.util.*

val UNINITIALIZED_METADATA_ENTITY_SET_ID = UUID(0, 0)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class OrganizationMetadataEntitySetIds(
    val organization: UUID = UNINITIALIZED_METADATA_ENTITY_SET_ID,
    val datasets: UUID = UNINITIALIZED_METADATA_ENTITY_SET_ID,
    val columns: UUID = UNINITIALIZED_METADATA_ENTITY_SET_ID
) : Serializable {

    val isInitialized: Boolean
        @JsonIgnore
        get() = listOf(
            columns,
            datasets,
            organization
        ).any { it == UNINITIALIZED_METADATA_ENTITY_SET_ID }
}
