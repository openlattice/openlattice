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

package com.openlattice.analysis

import com.openlattice.analysis.requests.FilteredRanking
import com.openlattice.edm.type.PropertyType
import java.util.*

/**
 * Contains the results of authorizing a filtered ranking.
 */
class AuthorizedFilteredRanking(
        val filteredRanking: FilteredRanking,
        val associationSets: Map<UUID, Set<UUID>>,
        val associationPropertyTypes: Map<UUID, PropertyType>,
        val entitySets: Map<UUID, Set<UUID>>,
        val entitySetPropertyTypes: Map<UUID, PropertyType>
)

