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
import com.openlattice.analysis.requests.Calculation
import com.openlattice.analysis.requests.OrientedPropertyFilter
import com.openlattice.client.serialization.SerializationConstants
import java.util.*

data class AssemblyAggregationFilter(
        @JsonProperty(SerializationConstants.ORGANIZATION_ID) val organizationId: UUID,
        @JsonProperty(SerializationConstants.SRC_ENTITY_SET_ID) val srcEntitySetId: UUID,
        @JsonProperty(SerializationConstants.EDGE_ENTITY_SET_ID) val edgeEntitySetId: UUID,
        @JsonProperty(SerializationConstants.DST_ENTITY_SET_ID) val dstEntitySetId: UUID,
        @JsonProperty(SerializationConstants.GROUPINGS) val groupProperties: List<OrientedPropertyTypeId>,
        @JsonProperty(SerializationConstants.AGGREGATIONS) val aggregations: List<Aggregation>,
        @JsonProperty(SerializationConstants.CALCULATIONS) val customCalculations: Set<Calculation>,
        @JsonProperty(SerializationConstants.FILTERS) val filters: List<OrientedPropertyFilter>)