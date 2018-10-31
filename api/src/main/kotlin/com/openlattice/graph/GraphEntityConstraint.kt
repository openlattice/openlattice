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

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.base.Preconditions.checkState
import com.openlattice.analysis.requests.Filter
import com.openlattice.client.serialization.SerializationConstants
import java.util.*

/**
 *
 * Used to constraint the entity types and entity sets that for a graph query. It
 */
data class GraphEntityConstraint(
        @JsonProperty(SerializationConstants.ENTITY_TYPE_ID) val entityTypeId: UUID,
        @JsonProperty(SerializationConstants.ENTITY_SET_IDS) private val maybeEntitySetIds: Optional<MutableSet<UUID>>,
        @JsonProperty(SerializationConstants.FILTERS) val filters: Map<UUID, Set<Filter>>
) {
    val entitySetIds : MutableSet<UUID> = maybeEntitySetIds.orElse( mutableSetOf() )
    fun updateEntitySetIds( entitySetIds: Set<UUID> ) {
        this.entitySetIds.clear()
        this.entitySetIds.addAll(entitySetIds)
    }
}