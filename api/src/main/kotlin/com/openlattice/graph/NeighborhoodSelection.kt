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

import com.openlattice.analysis.requests.Filter
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class NeighborhoodSelection(
        val entityTypeId: Optional<Set<UUID>>,
        val entitySetIds: Optional<Set<UUID>>,
        val associationTypeId: Optional<Set<UUID>>,
        val associationEntitySetIds: Optional<Set<UUID>>,
        val entityFilters: Optional<Map<UUID,Map<UUID,Filter>>>,
        val associationFilters: Optional<Map<UUID, Map<UUID,Filter>>>
) {
    init{
        check( entityTypeId.isPresent xor entitySetIds.isPresent ) {
            "Either entity entity type id or entity set ids must be present."
        }

        check( associationTypeId.isPresent xor associationEntitySetIds.isPresent ) {
            "Either asssociation entity type id or entity set ids must be present."
        }
    }
}