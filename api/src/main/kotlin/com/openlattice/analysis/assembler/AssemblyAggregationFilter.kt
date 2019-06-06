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
package com.openlattice.analysis.assembler

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.analysis.requests.AggregationType
import com.openlattice.client.serialization.SerializationConstants
import java.util.*

data class AssemblyAggregationFilter(
        @JsonProperty(SerializationConstants.ORGANIZATION_ID)val organizationId: UUID,
        @JsonProperty(SerializationConstants.SRC_ENTITY_SET_ID)val srcEntitySetId: UUID,
        @JsonProperty(SerializationConstants.EDGE_ENTITY_SET_ID)val edgeEntitySetId: UUID,
        @JsonProperty(SerializationConstants.DST_ENTITY_SET_ID)val dstEntitySetId: UUID,
        @JsonProperty(SerializationConstants.SRC_GROUP_PROPERTIES)val srcGroupProperties: Set<UUID>,
        @JsonProperty(SerializationConstants.EDGE_GROUP_PROPERTIES)val edgeGroupProperties: Set<UUID>,
        @JsonProperty(SerializationConstants.DST_GROUP_PROPERTIES)val dstGroupProperties: Set<UUID>,
        @JsonProperty(SerializationConstants.SRC_AGGREGATIONS)val srcAggregations: Map<UUID, Set<AggregationType>>,
        @JsonProperty(SerializationConstants.EDGE_AGGREGATIONS)val edgeAggregations: Map<UUID, Set<AggregationType>>,
        @JsonProperty(SerializationConstants.DST_AGGREGATIONS)val dstAggregations: Map<UUID, Set<AggregationType>>)