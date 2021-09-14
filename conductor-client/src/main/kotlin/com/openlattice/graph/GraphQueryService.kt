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

package com.openlattice.graph

import com.openlattice.analysis.requests.Filter
import com.openlattice.edm.type.PropertyType
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface GraphQueryService {
    fun submitQuery(
            query: NeighborhoodQuery,
            propertyTypes: Map<UUID, PropertyType>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            filter: Optional<Filter> = Optional.empty()
    ): Neighborhood

    /**
     * Looks up the entity sets of a set of entity key ids.
     * @param ids The entity key ids to lookup
     * @return Returns a map of entity key id to entity set id.
     */
    fun getEntitySetForIds(ids: Set<UUID>): Map<UUID, UUID>

    fun getEntitySets(entityTypeIds: Optional<Set<UUID>>): List<UUID>
}